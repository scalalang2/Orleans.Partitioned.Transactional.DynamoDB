namespace Orleans.Partitioned.Transactional.DynamoDB.Tests.PartitionedStateTest.Grains;

public interface ITestCoordinatorGrain : IGrainWithGuidKey
{
    [Transaction(TransactionOption.Create)]
    Task MultiGrainSet(List<ITestGrain> grains, Dictionary<string, int> items);

    [Transaction(TransactionOption.Create)]
    Task MultiGrainSetAndThrow(
        List<ITestGrain> throwGrains,
        List<ITestGrain> grains,
        Dictionary<string, int> items);
}
