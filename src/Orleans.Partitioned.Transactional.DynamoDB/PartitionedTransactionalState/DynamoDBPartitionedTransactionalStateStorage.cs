using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Partitioned.Transactional.DynamoDB.Internal;
using Orleans.Partitioned.Transactional.DynamoDB.Shared;
using Orleans.Storage;
using Orleans.Transactions;
using Orleans.Transactions.Abstractions;

namespace Orleans.Partitioned.Transactional.DynamoDB.PartitionedTransactionalState;

public partial class DynamoDBPartitionedTransactionalStateStorage<TState> : ITransactionalStateStorage<TState>
    where TState : class, IPartitionedState, new()
{
    private readonly TransactionalDynamoDBStorage storage;
    private readonly DynamoDBTransactionalStorageOptions options;
    private readonly string tableName;
    private readonly string partitionKey; // main state PK: <grainId>/<grainType>_<serviceId>_<stateName>
    private readonly IGrainStorageSerializer serializer;
    private readonly ILogger<DynamoDBPartitionedTransactionalStateStorage<TState>> logger;

    private readonly Type keyType;
    private readonly Type valueType;
    private readonly Type dictType; // Dictionary<TKey, TValue>
    private readonly System.Reflection.PropertyInfo itemsProperty; // IPartitionedState<TKey,TValue>.Items

    private KeyEntity key;
    private List<KeyValuePair<long, StateEntity>> states;
    private PartitionManifest currentManifest;
    private int committedPartitionSize;
    
    public DynamoDBPartitionedTransactionalStateStorage(
        TransactionalDynamoDBStorage storage,
        DynamoDBTransactionalStorageOptions options,
        string partitionKey,
        ILogger<DynamoDBPartitionedTransactionalStateStorage<TState>> logger)
    {
        this.storage = storage;
        this.options = options;
        this.tableName = options.TableName;
        this.partitionKey = partitionKey;
        this.serializer = options.GrainStorageSerializer;
        this.logger = logger;

        if (!PartitionedStateHelper.IsPartitionedState(typeof(TState)))
            throw new InvalidOperationException(
                $"Type {typeof(TState)} does not implement IPartitionedState<TKey, TValue>. " +
                "Use DynamoDBTransactionalStateStorage for non-partitioned state.");

        (keyType, valueType) = PartitionedStateHelper.GetPartitionTypeArgs(typeof(TState));
        dictType = typeof(SortedDictionary<,>).MakeGenericType(keyType, valueType);
        var ifaceType = typeof(IPartitionedState<,>).MakeGenericType(keyType, valueType);
        itemsProperty = ifaceType.GetProperty(nameof(IPartitionedState<int, int>.Items))!;
    }
    
    public async Task<TransactionalStorageLoadResponse<TState>> Load()
    {
        try
        {
            // Load key entity and state from main PK
            var keyEntityTask = LoadKeyEntityAsync();
            var stateEntitiesTask = LoadStateEntitiesAsync();
            key = await keyEntityTask.ConfigureAwait(false);
            states = await stateEntitiesTask.ConfigureAwait(false);

            if (string.IsNullOrEmpty(key.ETag.ToString()))
            {
                LogDebugLoadedFresh(this.partitionKey);
                currentManifest = new PartitionManifest();
                committedPartitionSize = 0;
                return new TransactionalStorageLoadResponse<TState>();
            }

            TState committedState;
            if (this.key.CommittedSequenceId == 0)
            {
                committedState = new TState();
                currentManifest = new PartitionManifest();
                committedPartitionSize = 0;
            }
            else
            {
                if (!FindState(this.key.CommittedSequenceId, out var pos))
                {
                    var error =
                        $"Storage state corrupted: no record for committed state v{this.key.CommittedSequenceId}";
                    LogCritical(this.partitionKey, error);
                    throw new InvalidOperationException(error);
                }

                committedState = await ReassembleStateAsync(states[pos].Value, updateCurrentCommittedState: true);
            }

            var pendingRecords = new List<PendingTransactionState<TState>>();
            for (var i = 0; i < states.Count; i++)
            {
                var kvp = states[i];

                // pending states for already committed transactions can be ignored
                if (kvp.Key <= key.CommittedSequenceId)
                    continue;

                // upon recovery, local non-committed transactions are considered aborted
                if (kvp.Value.TransactionManager.Length == 0)
                    break;

                var tm = Deserialize<ParticipantId>(kvp.Value.TransactionManager);
                pendingRecords.Add(new PendingTransactionState<TState>()
                {
                    SequenceId = kvp.Key,
                    State = await ReassembleStateAsync(kvp.Value),
                    TimeStamp = kvp.Value.TransactionTimestamp,
                    TransactionId = kvp.Value.TransactionId,
                    TransactionManager = tm,
                });
            }

            // no longer needed, ok to GC now
            for (var i = 0; i < states.Count; i++)
            {
                var entity = states[i].Value;
                entity.State = []; // clear the state to free memory
            }

            var metadata = Deserialize<TransactionalStateMetaData>(this.key.Metadata);
            return new TransactionalStorageLoadResponse<TState>(this.key.ETag.ToString(), committedState,
                this.key.CommittedSequenceId, metadata, pendingRecords);
        }
        catch (Exception ex)
        {
            this.logger.LogError(ex, "Error loading transactional state for partition key {PartitionKey}", this.partitionKey);
            throw;
        }
    }
    
    public async Task<string> Store(
        string expectedETag, 
        TransactionalStateMetaData metadata, 
        List<PendingTransactionState<TState>> statesToPrepare, 
        long? commitUpTo, 
        long? abortAfter)
    {
        try
        {
            var keyETag = key.ETag.ToString();
            if ((!string.IsNullOrWhiteSpace(keyETag) || !string.IsNullOrWhiteSpace(expectedETag)) && keyETag != expectedETag)
            {
                throw new ArgumentException(nameof(expectedETag), "Etag does not match");
            }

            if (abortAfter.HasValue && states.Count != 0)
            {
                while (states.Count > 0 && states[states.Count - 1].Key > abortAfter)
                {
                    var entity = states[states.Count - 1].Value;
                    
                    // Delete partition rows belonging to this aborted header (on separate PKs)
                    await DeletePartitionsOfAbortedEntity(entity);

                    await this.storage.DeleteEntryAsync(
                        this.tableName,
                        this.MakeKeyAttributes(entity.PartitionKey, entity.RowKey),
                        $"{SharedConstants.ETAG_PROPERTY_NAME} = :etag",
                        new Dictionary<string, AttributeValue>
                        {
                            [":etag"] = new AttributeValue { N = entity.ETag.ToString() }
                        });

                    states.RemoveAt(states.Count - 1);
                    LogTraceDelete(entity.PartitionKey, entity.RowKey, entity.TransactionId);
                }
            }
            
            // Prepare states
            var obsoleteBefore = commitUpTo ?? key.CommittedSequenceId;
            if (statesToPrepare != null)
            {
                foreach (var s in statesToPrepare)
                {
                    if (s.SequenceId >= obsoleteBefore)
                    {
                        await PreparePartitionedState(s);
                    }
                }
            }
            
            // Update key entity (transaction metadata)
            // TODO PartitionManifest도 Key에서 관리하는게 어떨까
            PartitionManifest? oldManifest = null;
            key.Metadata = Serialize(metadata);
            key.Timestamp = DateTimeOffset.UtcNow;
            if (commitUpTo.HasValue && commitUpTo.Value > key.CommittedSequenceId)
            {
                oldManifest = currentManifest;
                key.CommittedSequenceId = commitUpTo.Value;

                if (FindState(commitUpTo.Value, out var committedPos))
                {
                    var committedState = states[committedPos].Value;
                    if (committedState.PartitionManifest is { Length: > 0 })
                    {
                        currentManifest = Deserialize<PartitionManifest>(committedState.PartitionManifest);
                    }

                    if (committedState.State is { Length: > 0 })
                    {
                        var tempState = Deserialize<TState>(committedState.State);
                        if (tempState != null)
                        {
                            committedPartitionSize = tempState.PartitionSize;
                        }
                    }
                }
            }

            var existingETag = key.ETag.ToString();
            if (string.IsNullOrWhiteSpace(existingETag))
            {
                this.key.ETag = 0;
                await this.storage.PutEntryAsync(tableName, key.ToStorageFormat());
                LogTraceInsertKey(partitionKey, KeyEntity.RK, this.key.CommittedSequenceId, metadata.CommitRecords.Count);
            }
            else
            {
                this.key.ETag = this.key.ETag + 1;
                await this.storage.PutEntryAsync(
                    tableName,
                    key.ToStorageFormat(),
                    "ETag = :etag",
                    new Dictionary<string, AttributeValue> { [":etag"] = new AttributeValue { N = existingETag } });
                LogTraceUpdateKey(partitionKey, KeyEntity.RK, this.key.CommittedSequenceId, metadata.CommitRecords.Count);
            }
            
            if (oldManifest != null)
            {
                await CleanupOrphanPartitions(oldManifest, currentManifest);
            }
            
            // delete obsolete states and their orphan partitions
            if (states.Count > 0 && states[0].Key < obsoleteBefore)
            {
                FindState(obsoleteBefore, out var pos);
                for (int i = 0; i < pos; i++)
                {
                    var stateToDelete = states[i];

                    await DeleteOrphanPartitionRows(stateToDelete.Value);

                    await this.storage.DeleteEntryAsync(
                        this.tableName,
                        this.MakeKeyAttributes(stateToDelete.Value.PartitionKey, stateToDelete.Value.RowKey),
                        $"{SharedConstants.ETAG_PROPERTY_NAME} = :etag",
                        new Dictionary<string, AttributeValue>
                        {
                            [":etag"] = new AttributeValue { N = stateToDelete.Value.ETag.ToString() }
                        });

                    LogTraceDelete(this.partitionKey, states[i].Value.RowKey, states[i].Value.TransactionId);
                }

                states.RemoveRange(0, pos);
            }

            LogDebugStored(this.partitionKey, this.key.CommittedSequenceId, this.key.ETag);
            return key.ETag.ToString();
        }
        catch (Exception ex)
        {
            LogErrorStoreFailed(logger, ex);
            throw;
        }
    }

    private async Task PreparePartitionedState(PendingTransactionState<TState> pendingState)
    {
        var state = pendingState.State;

        var (items, partitionSize) = ExtractPartitionedFields(state);

        var partitions = PartitionedStateHelper.SplitIntoPartitions(items, partitionSize, dictType);

        var newManifest = new PartitionManifest();
        
        var rebalanceNeeded = currentManifest == null || this.committedPartitionSize != partitionSize;

        // TODO : 파티션 저장 단계에서는 성공한 뒤, StateEntity에서 실패한다면 정리되지 않는 Partition Row가 발생함
        foreach (var (partitionNumber, partitionData) in partitions)
        {
            var serializedPartition = SerializeObject(partitionData);
            var hash = PartitionedStateHelper.ComputeHash(serializedPartition);

            var isChanged = rebalanceNeeded
                            || currentManifest?.PartitionInfos == null
                            || !currentManifest.PartitionInfos.TryGetValue(partitionNumber, out var oldInfo)
                            || oldInfo.HashCode != hash;

            if (isChanged)
            {
                newManifest.PartitionInfos[partitionNumber] = new PartitionInfo { CommitSeq = pendingState.SequenceId, HashCode = hash };

                var partitionEntity = new PartitionEntity
                {
                    PartitionKey = PartitionEntity.MakePartitionKey(this.partitionKey, partitionNumber),
                    RowKey = PartitionEntity.MakeRowKey(pendingState.SequenceId),
                    Data = serializedPartition,
                    ETag = 0,
                };
                
                await this.storage.PutEntryAsync(this.tableName, partitionEntity.ToStorageFormat());
                LogTracePartitionWrite(partitionEntity.PartitionKey, partitionEntity.RowKey, partitionNumber);
            }
            else
            {
                // Unchanged
                if (currentManifest?.PartitionInfos != null &&
                    currentManifest.PartitionInfos.TryGetValue(partitionNumber, out oldInfo))
                {
                    newManifest.PartitionInfos[partitionNumber] = oldInfo;
                }
            }
        }

        var savedItems = GetItems(state);
        
        // TODO: 저장소에는 Items를 저장하지 않기 위한 방법이지만, 접근법이 마음데 들진 않는다.
        // 직렬화 단계에서 Items만 무시하고 직렬화 하는 함수를 정의하면 어떨까
        ClearItems(state);
        var payload = Serialize(state);
        var manifestPayload = Serialize(newManifest);
        SetItems(state, savedItems); // restore items

        if (FindState(pendingState.SequenceId, out var pos))
        {
            var existing = states[pos].Value;
            var currentETag = existing.ETag.ToString();
            existing.TransactionId = pendingState.TransactionId;
            existing.TransactionTimestamp = pendingState.TimeStamp;
            existing.TransactionManager = Serialize(pendingState.TransactionManager);
            existing.State = payload;
            existing.PartitionManifest = manifestPayload;
            existing.ETag = existing.ETag + 1;

            await this.storage.UpsertEntryAsync(
                tableName,
                existing.KeyAttributes(),
                existing.UpdatedFieldsAttributes(),
                $"{SharedConstants.ETAG_PROPERTY_NAME} = :etag",
                new Dictionary<string, AttributeValue>
                {
                    [":etag"] = new AttributeValue { N = currentETag }
                });

            LogTraceUpdate(partitionKey, existing.RowKey, existing.TransactionId);
        }
        else
        {
            // Insert new header with pre-serialized bytes
            var entity = new StateEntity
            {
                PartitionKey = this.partitionKey,
                RowKey = StateEntity.MakeRowKey(pendingState.SequenceId),
                TransactionId = pendingState.TransactionId,
                TransactionTimestamp = pendingState.TimeStamp,
                TransactionManager = Serialize(pendingState.TransactionManager),
                State = payload,
                PartitionManifest = manifestPayload,
                ETag = 0,
            };

            await this.storage.PutEntryAsync(tableName, entity.ToStorageFormat());
            states.Insert(pos, new KeyValuePair<long, StateEntity>(pendingState.SequenceId, entity));

            LogTraceInsert(partitionKey, entity.RowKey, entity.TransactionId);
        }
    }

    /// <summary>
    /// Reassemble full TState
    /// </summary>
    private async Task<TState> ReassembleStateAsync(StateEntity stateEntity, bool updateCurrentCommittedState = false)
    {
        var state = Deserialize<TState>(stateEntity.State);
        if (state == null)
        {
            state = new TState();
        }

        var manifest = stateEntity.PartitionManifest is { Length: > 0 } 
            ? Deserialize<PartitionManifest>(stateEntity.PartitionManifest) 
            : new PartitionManifest();

        if (updateCurrentCommittedState)
        {
            currentManifest = manifest;
            committedPartitionSize = state.PartitionSize;
        }

        if (manifest.PartitionInfos.Count == 0)
        {
            return state;
        }

        // TODO: wrap partitioned entity as specific class (e.g. PartitionEntity<TKey, TValue>);
        var partitionData = await LoadPartitionRowsAsync(manifest);
        var merged = (IDictionary)Activator.CreateInstance(dictType)!;

        foreach (var (partitionNumber, data) in partitionData)
        {
            if (data != null && data.Length > 0)
            {
                var partDict = (IDictionary)DeserializeObject(dictType, data);
                foreach (DictionaryEntry entry in partDict)
                {
                    merged[entry.Key] = entry.Value;
                }
            }
            else
            {
                var commitSeq = manifest.PartitionInfos[partitionNumber].CommitSeq;
                LogErrorMissingPartition(this.partitionKey, partitionNumber, commitSeq);
                throw new Exception($"Missing partition {partitionNumber} at sequence {commitSeq} for grain {this.partitionKey}");
            }
        }
        
        SetItems(state, merged);

        return state;
    }

    private async Task<List<(uint partitionNumber, byte[] data)>> LoadPartitionRowsAsync(PartitionManifest manifest)
    {
        var result = new List<(uint partitionNumber, byte[] data)>();

        var keysToRead = new List<(uint num, Dictionary<string, AttributeValue> keys)>();
        foreach (var (partNum, info) in manifest.PartitionInfos)
        {
            var pk = PartitionEntity.MakePartitionKey(this.partitionKey, partNum);
            var rk = PartitionEntity.MakeRowKey(info.CommitSeq);
            keysToRead.Add((partNum, MakeKeyAttributes(pk, rk)));
        }

        if (keysToRead.Count == 0)
        {
            return result;
        }

        var batchKeys = keysToRead.Select(k => k.keys);
        var entities = await this.storage.GetEntriesTxAsync(this.tableName,
            batchKeys,
            fields => new PartitionEntity(fields)).ConfigureAwait(false);

        var entityLookup = new Dictionary<string, PartitionEntity>();
        foreach (var entity in entities)
        {
            if (entity != null)
            {
                entityLookup[entity.PartitionKey] = entity;
            }
        }

        foreach (var (partNum, _) in keysToRead)
        {
            var pk = PartitionEntity.MakePartitionKey(this.partitionKey, partNum);
            if (entityLookup.TryGetValue(pk, out var entity))
            {
                result.Add((partNum, entity.Data));
            }
            else
            {
                result.Add((partNum, null));
            }
        }

        return result;
    }

    private async Task<KeyEntity> LoadKeyEntityAsync()
    {
        var keyAttributes = new Dictionary<string, AttributeValue>
        {
            [SharedConstants.PARTITION_KEY_PROPERTY_NAME] = new AttributeValue { S = partitionKey },
            [SharedConstants.ROW_KEY_PROPERTY_NAME] = new AttributeValue { S = KeyEntity.RK },
        };

        var keyEntity = await storage.ReadSingleEntryAsync(
            tableName,
            keyAttributes,
            (item) => new KeyEntity(item)).ConfigureAwait(false);
        return keyEntity ?? new KeyEntity(this.partitionKey);
    }
    
    /// <summary>
    /// Loads all unpublished StateEntity records from DynamoDB.
    /// </summary>
    private async Task<List<KeyValuePair<long, StateEntity>>> LoadStateEntitiesAsync()
    {
        var keyConditionExpression =
            $"{SharedConstants.PARTITION_KEY_PROPERTY_NAME} = :partitionKey and {SharedConstants.ROW_KEY_PROPERTY_NAME} between :minRowKeyPrefix and :maxRowKeyPrefix";
        var keys = new Dictionary<string, AttributeValue>
        {
            { ":partitionKey", new AttributeValue { S = this.partitionKey } },
            { ":minRowKeyPrefix", new AttributeValue { S = StateEntity.ROW_KEY_MIN } },
            { ":maxRowKeyPrefix", new AttributeValue { S = StateEntity.ROW_KEY_MAX } },
        };

        var records = new List<StateEntity>();
        try
        {
            records = await this.storage.QueryAllAsync(
                this.tableName,
                keys,
                keyConditionExpression,
                (fields) => new StateEntity(fields)).ConfigureAwait(false);

            return records.Select(record => new KeyValuePair<long, StateEntity>(record.SequenceId, record)).ToList();
        }
        catch (Exception ex)
        {
            this.logger.LogError($"Read transactional states failed {ex}.");
            throw;
        }
    }
    
    private (IDictionary items, int partitionSize) ExtractPartitionedFields(TState state)
    {
        var items = (IDictionary)itemsProperty.GetValue(state)!;
        return (items, state.PartitionSize);
    }
    
    private IDictionary GetItems(TState state)
        => (IDictionary)itemsProperty.GetValue(state)!;
    
    private void SetItems(TState state, IDictionary items)
        => itemsProperty.SetValue(state, items);

    private void ClearItems(TState state)
        => SetItems(state, (IDictionary)Activator.CreateInstance(dictType)!);

    private async Task CleanupOrphanPartitions(PartitionManifest oldManifest, PartitionManifest newManifest)
    {
        var keysToDelete = new List<Dictionary<string, AttributeValue>>();

        foreach (var (pNumber, oldInfo) in oldManifest.PartitionInfos)
        {
            if (newManifest.PartitionInfos.TryGetValue(pNumber, out var newInfo))
            {
                if (newInfo.CommitSeq == oldInfo.CommitSeq)
                {
                    continue;
                }
                
                var pk = PartitionEntity.MakePartitionKey(this.partitionKey, pNumber);
                var rk = PartitionEntity.MakeRowKey(oldInfo.CommitSeq);
                keysToDelete.Add(MakeKeyAttributes(pk, rk));
            }
            else
            {
                // Partition was removed entirely
                var pk = PartitionEntity.MakePartitionKey(this.partitionKey, pNumber);
                var rk = PartitionEntity.MakeRowKey(oldInfo.CommitSeq);
                keysToDelete.Add(MakeKeyAttributes(pk, rk));
            }
        }
        
        if (keysToDelete.Count > 0)
        {
            // TODO: 이미 지워진 객체를 다시 한 번 지울 가능성이 있음
            await this.storage.DeleteEntriesAsync(this.tableName, keysToDelete);
        }
    }
    
    private async Task DeletePartitionsOfAbortedEntity(StateEntity stateEntity)
    {
        if (stateEntity.PartitionManifest is not { Length: > 0 }) return;

        try
        {
            var manifest = Deserialize<PartitionManifest>(stateEntity.PartitionManifest);
            if (manifest == null)
            {
                return;
            }

            var keysToDelete = new List<Dictionary<String, AttributeValue>>();

            foreach (var (partNum, info) in manifest.PartitionInfos)
            {
                // delete only if partition's sequenceId is equal to sequenceId to be removed.
                if (info.CommitSeq != stateEntity.SequenceId)
                {
                    continue;
                }

                var pk = PartitionEntity.MakePartitionKey(this.partitionKey, partNum);
                var rk = PartitionEntity.MakeRowKey(info.CommitSeq);
                keysToDelete.Add(MakeKeyAttributes(pk, rk));
            }

            if (keysToDelete.Count > 0)
            {
                await this.storage.DeleteEntriesAsync(this.tableName, keysToDelete);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to delete partitions in aborted state {PartitionKey}/{RowKey}", this.partitionKey, stateEntity.RowKey);
        }
    }

    /// <summary>
    /// Deletes orphan partition rows from an old header that are no longer referenced by currentManifest.
    /// </summary>
    private async Task DeleteOrphanPartitionRows(StateEntity stateEntity)
    {
        if (stateEntity.PartitionManifest is not { Length: > 0 }) return;

        try
        {
            var oldManifest = Deserialize<PartitionManifest>(stateEntity.PartitionManifest);
            if (oldManifest == null) return;
            
            var keysToDelete = new List<Dictionary<string, AttributeValue>>();
            
            foreach (var (partNum, oldInfo) in oldManifest.PartitionInfos)
            {
                // Don't delete if current manifest still references this exact (partNum, seq) pair
                if (currentManifest?.PartitionInfos != null &&
                    currentManifest.PartitionInfos.TryGetValue(partNum, out var curInfo) &&
                    curInfo.CommitSeq == oldInfo.CommitSeq)
                {
                    continue; // still in use
                }

                var pk = PartitionEntity.MakePartitionKey(this.partitionKey, partNum);
                var rk = PartitionEntity.MakeRowKey(oldInfo.CommitSeq);
                keysToDelete.Add(MakeKeyAttributes(pk, rk));
            }

            if (keysToDelete.Count > 0)
            {
                await this.storage.DeleteEntriesAsync(this.tableName, keysToDelete);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to cleanup orphan partitions for old state {PartitionKey}/{RowKey}", this.partitionKey, stateEntity.RowKey);
        }
    }
    
    private Dictionary<string, AttributeValue> MakeKeyAttributes(string pk, string rowKey)
    {
        return new Dictionary<string, AttributeValue>
        {
            [SharedConstants.PARTITION_KEY_PROPERTY_NAME] = new AttributeValue { S = pk },
            [SharedConstants.ROW_KEY_PROPERTY_NAME] = new AttributeValue { S = rowKey }
        };
    }
    
    private bool FindState(long sequenceId, out int pos)
    {
        pos = 0;
        while (pos < states.Count)
        {
            switch (states[pos].Key.CompareTo(sequenceId))
            {
                case 0:
                    return true;
                case -1:
                    pos++;
                    continue;
                case 1:
                    return false;
            }
        }
        return false;
    }
    
    private T Deserialize<T>(byte[] value)
    {
        T dataValue = default;
        try
        {
            if (value is { Length: > 0 })
                dataValue = this.serializer.Deserialize<T>(value);
        }
        catch (Exception exc)
        {
            var sb = new StringBuilder();
            sb.AppendFormat("Unable to deserialize the vlaue");

            if (dataValue != null)
            {
                sb.Append($"Data Value={dataValue} Type={dataValue.GetType()}");
            }

            var message = sb.ToString();
            LogError(logger, message);
            throw new AggregateException(message, exc);
        }

        return dataValue;
    }
    
    private byte[] Serialize<T>(T value) => this.serializer.Serialize(value).ToArray();
    
    private byte[] SerializeObject(object value)
    {
        var method = typeof(IGrainStorageSerializer).GetMethod(nameof(IGrainStorageSerializer.Serialize))!
            .MakeGenericMethod(value.GetType());
        var result = (BinaryData)method.Invoke(this.serializer, [value]);
        return result.ToArray();
    }

    private object DeserializeObject(Type type, byte[] data)
    {
        var method = typeof(IGrainStorageSerializer).GetMethod(nameof(IGrainStorageSerializer.Deserialize))!
            .MakeGenericMethod(type);
        return method.Invoke(this.serializer, [new BinaryData(data)]);
    }
    
    #region Logging
    
    [LoggerMessage(Level = LogLevel.Error, Message = "{Message}")]
    private static partial void LogError(ILogger logger, string message);

    [LoggerMessage(Level = LogLevel.Debug, Message = "{Partition} Loaded fresh (no prior state)")]
    private partial void LogDebugLoadedFresh(string partition);

    [LoggerMessage(Level = LogLevel.Critical, Message = "{Partition} {Error}")]
    private partial void LogCritical(string partition, string error);

    [LoggerMessage(Level = LogLevel.Trace, Message = "{PartitionKey}.{RowKey} Delete {TransactionId}")]
    private partial void LogTraceDelete(string partitionKey, string rowKey, string transactionId);

    [LoggerMessage(Level = LogLevel.Trace, Message = "{PartitionKey}.{RowKey} Update {TransactionId}")]
    private partial void LogTraceUpdate(string partitionKey, string rowKey, string transactionId);

    [LoggerMessage(Level = LogLevel.Trace, Message = "{PartitionKey}.{RowKey} Insert {TransactionId}")]
    private partial void LogTraceInsert(string partitionKey, string rowKey, string transactionId);

    [LoggerMessage(Level = LogLevel.Trace, Message = "{PartitionKey}.{RowKey} Insert key. v{CommittedSequenceId}, {CommitRecordsCount}c")]
    private partial void LogTraceInsertKey(string partitionKey, string rowKey, long committedSequenceId, int commitRecordsCount);

    [LoggerMessage(Level = LogLevel.Trace, Message = "{PartitionKey}.{RowKey} Update key. v{CommittedSequenceId}, {CommitRecordsCount}c")]
    private partial void LogTraceUpdateKey(string partitionKey, string rowKey, long committedSequenceId, int commitRecordsCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "{PartitionKey} Stored v{CommittedSequenceId} eTag={ETag}")]
    private partial void LogDebugStored(string partitionKey, long committedSequenceId, long? eTag);

    [LoggerMessage(Level = LogLevel.Error, Message = "Partitioned transactional state store failed.")]
    private static partial void LogErrorStoreFailed(ILogger logger, Exception ex);

    [LoggerMessage(Level = LogLevel.Trace, Message = "{PartitionKey}.{RowKey} Wrote partition {PartitionNumber}")]
    private partial void LogTracePartitionWrite(string partitionKey, string rowKey, uint partitionNumber);

    [LoggerMessage(Level = LogLevel.Error, Message = "{PartitionKey} Missing partition entity: partition={PartitionNumber} commitSeq={CommitSeq}")]
    private partial void LogErrorMissingPartition(string partitionKey, uint partitionNumber, long commitSeq);

    #endregion
}
