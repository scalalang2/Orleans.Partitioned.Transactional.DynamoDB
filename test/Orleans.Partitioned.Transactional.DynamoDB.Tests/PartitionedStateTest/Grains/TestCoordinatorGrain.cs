namespace Orleans.Partitioned.Transactional.DynamoDB.Tests.PartitionedStateTest.Grains;

public class TestCoordinatorGrain : Grain, ITestCoordinatorGrain
{
    public async Task MultiGrainSet(List<ITestGrain> grains, Dictionary<string, int> items)
    {
        await Task.WhenAll(grains.Select(g => g.SetMultiple(items)));   
    }

    public async Task MultiGrainSetAndThrow(
        List<ITestGrain> throwGrains,
        List<ITestGrain> grains,
        Dictionary<string, int> items)
    {
        await Task.WhenAll(grains.Select(g => g.SetMultiple(items)));
        await Task.WhenAll(throwGrains.Select(g => g.SetMultipleAndThrow(items)));
    }
}