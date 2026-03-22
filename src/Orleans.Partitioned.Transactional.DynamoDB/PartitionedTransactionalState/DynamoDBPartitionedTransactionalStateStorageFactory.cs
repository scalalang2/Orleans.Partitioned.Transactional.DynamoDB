using System.Diagnostics;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Partitioned.Transactional.DynamoDB.Internal;
using Orleans.Partitioned.Transactional.DynamoDB.Shared;
using Orleans.Transactions.Abstractions;

namespace Orleans.Partitioned.Transactional.DynamoDB.PartitionedTransactionalState;

public partial class DynamoDBPartitionedTransactionalStateStorageFactory : ITransactionalStateStorageFactory, ILifecycleParticipant<ISiloLifecycle>
{
    private readonly string name;
    private readonly DynamoDBTransactionalStorageOptions options;
    private readonly ClusterOptions clusterOptions;
    private readonly ILoggerFactory loggerFactory;

    private TransactionalDynamoDBStorage storage;

    public DynamoDBPartitionedTransactionalStateStorageFactory(
        string name,
        DynamoDBTransactionalStorageOptions options,
        IOptions<ClusterOptions> clusterOptions,
        IServiceProvider services,
        ILoggerFactory loggerFactory)
    {
        this.name = name;
        this.options = options;
        this.clusterOptions = clusterOptions.Value;
        this.loggerFactory = loggerFactory;
    }
    
    public static ITransactionalStateStorageFactory Create(IServiceProvider services, string name)
    {
        var optionsMonitor = services.GetRequiredService<IOptionsMonitor<DynamoDBTransactionalStorageOptions>>();
        return ActivatorUtilities.CreateInstance<DynamoDBPartitionedTransactionalStateStorageFactory>(services, name, optionsMonitor.Get(name));
    }
    
    public ITransactionalStateStorage<TState> Create<TState>(string stateName, IGrainContext context) where TState : class, new()
    {
        if (this.storage == null)
        {
            throw new ArgumentException("DynamoDBStorage client is not initialized");
        }
        
        if (!typeof(IPartitionedState).IsAssignableFrom(typeof(TState)))
        {
            throw new InvalidOperationException(
                $"Type {typeof(TState)} does not implement IPartitionedState. " +
                "Use DynamoDBTransactionalStateStorage for non-partitioned state.");
        }

        var partitionKey = this.MakePartitionKey(context, stateName);
        var storageType = typeof(DynamoDBPartitionedTransactionalStateStorage<>).MakeGenericType(typeof(TState));
        return (ITransactionalStateStorage<TState>)ActivatorUtilities.CreateInstance(
            context.ActivationServices,
            storageType,
            this.storage,
            this.options,
            partitionKey);
    }

    public void Participate(ISiloLifecycle lifecycle)
    {
        lifecycle.Subscribe(
            OptionFormattingUtilities.Name<DynamoDBPartitionedTransactionalStateStorageFactory>(this.name),
            this.options.InitStage,
            Init);
    }
    
    private async Task Initialize()
    {
        var stopWatch = Stopwatch.StartNew();
        var logger = this.loggerFactory.CreateLogger<TransactionalDynamoDBStorage>();

        try
        {
            var initMsg = string.Format("Init: Name={0} ServiceId={1} Table={2}", this.name, this.options.ServiceId, this.options.TableName);
            LogInformationInitializing(logger, this.name, initMsg);

            this.storage = new TransactionalDynamoDBStorage(
                logger,
                this.options.Service,
                this.options.AccessKey,
                this.options.SecretKey,
                this.options.Token,
                this.options.ProfileName,
                this.options.ReadCapacityUnits,
                this.options.WriteCapacityUnits,
                this.options.UseProvisionedThroughput,
                this.options.CreateIfNotExists,
                this.options.UpdateIfExists);

            await storage.InitializeTable(this.options.TableName,
                new List<KeySchemaElement>
                {
                    new KeySchemaElement { AttributeName = SharedConstants.PARTITION_KEY_PROPERTY_NAME, KeyType = KeyType.HASH },
                    new KeySchemaElement { AttributeName = SharedConstants.ROW_KEY_PROPERTY_NAME, KeyType = KeyType.RANGE }
                },
                new List<AttributeDefinition>
                {
                    new AttributeDefinition { AttributeName = SharedConstants.PARTITION_KEY_PROPERTY_NAME, AttributeType = ScalarAttributeType.S },
                    new AttributeDefinition { AttributeName = SharedConstants.ROW_KEY_PROPERTY_NAME, AttributeType = ScalarAttributeType.S }
                },
                secondaryIndexes: null,
                null);

            stopWatch.Stop();
            LogInformationInitialized(logger, this.name, this.GetType().Name, this.options.InitStage, stopWatch.ElapsedMilliseconds);
        }
        catch (Exception exc)
        {
            stopWatch.Stop();
            LogErrorInitFailed(logger, this.name, this.GetType().Name, this.options.InitStage, stopWatch.ElapsedMilliseconds, exc);
            throw;
        }
    }

    private string MakePartitionKey(IGrainContext context, string stateName)
    {
        var grainKey = context.GrainReference.GrainId.ToString();
        return $"{grainKey}_{this.clusterOptions.ServiceId}_{stateName}";
    }
    
    private Task Init(CancellationToken cancellationToken)
    {
        return Initialize();
    }
    
    [LoggerMessage(Level = LogLevel.Information, Message = "AWS DynamoDB Partitioned Transactional Grain Storage {Name} is initializing: {InitMsg}")]
    private static partial void LogInformationInitializing(ILogger logger, string name, string initMsg);

    [LoggerMessage(Level = LogLevel.Information, Message = "Initializing provider {Name} of type {Type} in stage {Stage} took {ElapsedMilliseconds} Milliseconds.")]
    private static partial void LogInformationInitialized(ILogger logger, string name, string type, int stage, long elapsedMilliseconds);

    [LoggerMessage(EventId = (int)ErrorCode.Provider_ErrorFromInit, Level = LogLevel.Error, Message = "Initialization failed for provider {Name} of type {Type} in stage {Stage} in {ElapsedMilliseconds} Milliseconds.")]
    private static partial void LogErrorInitFailed(ILogger logger, string name, string type, int stage, long elapsedMilliseconds, Exception exception);
}