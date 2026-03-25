using System.Collections.Generic;

namespace Orleans.Partitioned.Transactional.DynamoDB.PartitionedTransactionalState;

public interface IPartitionedState
{
    /// <summary>
    /// Number of partitions to split the Items
    /// </summary>
    int PartitionSize { get; set; }
}

public interface IPartitionedState<TKey, TValue> : IPartitionedState
{
    /// <summary>
    /// The actual data dictionary that will be split across partitions.
    /// </summary>
    SortedDictionary<TKey, TValue> Items { get; set; }
}