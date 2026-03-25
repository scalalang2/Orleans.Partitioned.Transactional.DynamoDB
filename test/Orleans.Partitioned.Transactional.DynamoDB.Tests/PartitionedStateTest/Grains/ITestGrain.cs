namespace Orleans.Partitioned.Transactional.DynamoDB.Tests.PartitionedStateTest.Grains;

public interface ITestGrain : IGrainWithGuidKey
{
    [Transaction(TransactionOption.CreateOrJoin)]
    Task Set(string key, int value);

    [Transaction(TransactionOption.CreateOrJoin)]
    Task SetMultiple(Dictionary<string, int> items);

    [Transaction(TransactionOption.CreateOrJoin)]
    Task<int?> Get(string key);

    [Transaction(TransactionOption.CreateOrJoin)]
    Task<Dictionary<string, int>> GetAll();

    [Transaction(TransactionOption.CreateOrJoin)]
    Task Add(string key, int delta);

    [Transaction(TransactionOption.CreateOrJoin)]
    Task Remove(string key);

    [Transaction(TransactionOption.CreateOrJoin)]
    Task RemoveMultiple(List<string> keys);

    /// <summary>Mutates then throws — forces transaction abort.</summary>
    [Transaction(TransactionOption.CreateOrJoin)]
    Task SetMultipleAndThrow(Dictionary<string, int> items);

    /// <summary>Removes then throws — forces transaction abort.</summary>
    [Transaction(TransactionOption.CreateOrJoin)]
    Task RemoveMultipleAndThrow(List<string> keys);
}
