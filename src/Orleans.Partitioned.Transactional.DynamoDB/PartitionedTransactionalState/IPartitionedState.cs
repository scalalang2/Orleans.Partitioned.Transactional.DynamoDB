namespace Orleans.Partitioned.Transactional.DynamoDB.PartitionedTransactionalState;

public interface IPartitionedState
{
    /// <summary>
    /// Number of partitions to split the Items
    /// </summary>
    int PartitionSize { get; set; }
    
    /// <summary>
    /// Partition metdata managed by the storage engine
    /// DO NOT set this field manually - the storage engine manages it.
    /// </summary>
    PartitionManifest Manifest { get; set; }
}

public interface IPartitionedState<TKey, TValue> : IPartitionedState
{
    /// <summary>
    /// The actual data dictionary that will be split across partitions.
    /// </summary>
    Dictionary<TKey, TValue> Items { get; set; }
}