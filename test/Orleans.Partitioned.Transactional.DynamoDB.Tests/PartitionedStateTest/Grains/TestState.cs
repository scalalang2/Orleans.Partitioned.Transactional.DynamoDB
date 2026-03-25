using Orleans.Partitioned.Transactional.DynamoDB.PartitionedTransactionalState;

namespace Orleans.Partitioned.Transactional.DynamoDB.Tests.PartitionedStateTest.Grains;

[GenerateSerializer]
public class TestState : IPartitionedState<string, int>
{
    [Id(0)] 
    public int PartitionSize { get; set; } = 3;
    
    [Id(1)] 
    public SortedDictionary<string, int> Items { get; set; } = new();
}