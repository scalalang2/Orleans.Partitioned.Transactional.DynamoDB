using Orleans.Partitioned.Transactional.DynamoDB.PartitionedTransactionalState;
using SampleApp.Abstractions;

namespace SampleApp.Grains;

[GenerateSerializer]
public class InventoryState : IPartitionedState<string, InventoryItem>
{
    [Id(0)] 
    public int PartitionSize { get; set; } = 5;

    [Id(1)] 
    public PartitionManifest Manifest { get; set; } = new();

    [Id(2)] 
    public SortedDictionary<string, InventoryItem> Items { get; set; } = new();
}