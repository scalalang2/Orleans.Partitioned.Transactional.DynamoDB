using System.Collections.Generic;

namespace Orleans.Partitioned.Transactional.DynamoDB.PartitionedTransactionalState;

/// <summary>
/// Manifest stored in the header row's GrainState field for partitioned states.
/// This tracks which partition was last written at which commit sequence. 
/// </summary>
[GenerateSerializer]
public class PartitionManifest
{
    /// <summary>
    /// To read partition K: PK = {basePK}_P{K}, RK = state_{commitSeq:x16}
    /// </summary>
    [Id(0)]
    public Dictionary<uint, long> PartitionToCommitSeq { get; set; } = new();

    /// <summary>
    /// Maps partition number to a SHA256 hash of the serialized partition data.
    /// This is used to detect which partition changed during `Store` operation.
    /// </summary>
    [Id(1)]
    public Dictionary<uint, string> PartitionHashCodes { get; set; } = new();
}