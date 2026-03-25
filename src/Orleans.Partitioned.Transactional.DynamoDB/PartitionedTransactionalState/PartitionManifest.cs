using System.Collections.Generic;

namespace Orleans.Partitioned.Transactional.DynamoDB.PartitionedTransactionalState;

/// <summary>
/// Info for a single partition.
/// </summary>
[GenerateSerializer]
public class PartitionInfo
{
    /// <summary>
    /// The commit sequence number for this partition.
    /// </summary>
    [Id(0)]
    public long CommitSeq { get; set; }

    /// <summary>
    /// The SHA256 hash of the serialized partition data.
    /// </summary>
    [Id(1)]
    public string HashCode { get; set; } = string.Empty;
}

/// <summary>
/// PartitionManifest stored in the field of primary state.
/// This tracks which partition was last written at which commit sequence. 
/// </summary>
[GenerateSerializer]
public class PartitionManifest
{
    /// <summary>
    /// Maps partition number to its info
    /// </summary>
    [Id(0)]
    public Dictionary<uint, PartitionInfo> PartitionInfos { get; set; } = new();
}