using System.Globalization;
using Amazon.DynamoDBv2.Model;
using Orleans.Partitioned.Transactional.DynamoDB.Shared;

namespace Orleans.Partitioned.Transactional.DynamoDB.PartitionedTransactionalState;

/// <summary>
/// This represents a partition row in DynamoDB
/// </summary>
internal class PartitionEntity
{
    internal PartitionEntity()
    {
    }

    internal PartitionEntity(Dictionary<string, AttributeValue> fields)
    {
        if (fields.TryGetValue(SharedConstants.PARTITION_KEY_PROPERTY_NAME, out var partitionKey))
            this.PartitionKey = partitionKey.S;

        if (fields.TryGetValue(SharedConstants.ROW_KEY_PROPERTY_NAME, out var rowKey))
            this.RowKey = rowKey.S;

        if (fields.TryGetValue(SharedConstants.BINARY_STATE_PROPERTY_NAME, out var state))
            this.Data = state.B.ToArray();

        if (fields.TryGetValue(SharedConstants.ETAG_PROPERTY_NAME, out var etag))
            this.ETag = int.Parse(etag.N);
    }
    
    public string PartitionKey { get; set; }

    public string RowKey { get; set; }

    public byte[] Data { get; set; }

    public long? ETag { get; set; }
    
     /// <summary>
    /// Sequence ID parsed from the RowKey (state_{hex16}).
    /// </summary>
    public long SequenceId => long.Parse(
        this.RowKey.Substring(StateEntity.ROW_KEY_PREFIX.Length),
        NumberStyles.AllowHexSpecifier);

    /// <summary>
    /// Builds the DynamoDB PartitionKey for a specific partition number.
    /// Format: {basePartitionKey}_p{partitionNumber}
    /// </summary>
    public static string MakePartitionKey(string basePartitionKey, uint partitionNumber)
        => $"{basePartitionKey}_p{partitionNumber}";

    /// <summary>
    /// Builds the RowKey for a partition row. Same format as header rows.
    /// Format: state_{seq:x16}
    /// </summary>
    public static string MakeRowKey(long sequenceId)
        => StateEntity.MakeRowKey(sequenceId);

    public Dictionary<string, AttributeValue> ToStorageFormat()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            { SharedConstants.PARTITION_KEY_PROPERTY_NAME, new AttributeValue { S = this.PartitionKey } },
            { SharedConstants.ROW_KEY_PROPERTY_NAME, new AttributeValue { S = this.RowKey } },
        };

        if (this.Data is { Length: > 0 })
            item[SharedConstants.BINARY_STATE_PROPERTY_NAME] = new AttributeValue { B = new MemoryStream(this.Data) };

        if (this.ETag.HasValue)
            item[SharedConstants.ETAG_PROPERTY_NAME] = new AttributeValue { N = this.ETag.Value.ToString() };

        return item;
    }

    public Dictionary<string, AttributeValue> KeyAttributes()
    {
        return new Dictionary<string, AttributeValue>
        {
            { SharedConstants.PARTITION_KEY_PROPERTY_NAME, new AttributeValue { S = this.PartitionKey } },
            { SharedConstants.ROW_KEY_PROPERTY_NAME, new AttributeValue { S = this.RowKey } },
        };
    }
}