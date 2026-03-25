using System.Runtime.Serialization;
using Orleans.Concurrency;
using Orleans.Transactions.Abstractions;

namespace Orleans.Partitioned.Transactional.DynamoDB.Tests.PartitionedStateTest.Grains;

[Reentrant]
public class TestGrain(
    [TransactionalState("data", "PartitionedTransactionStore")]
    ITransactionalState<TestState> state) : Grain, ITestGrain
{
    public Task Set(string key, int value)
        => state.PerformUpdate(s => s.Items[key] = value);

    public Task SetMultiple(Dictionary<string, int> items)
        => state.PerformUpdate(s => { foreach (var (k, v) in items) s.Items[k] = v; });

    public Task<int?> Get(string key)
        => state.PerformRead<int?>(s => s.Items.TryGetValue(key, out var v) ? v : null);

    public Task<Dictionary<string, int>> GetAll()
        => state.PerformRead(s => new Dictionary<string, int>(s.Items));

    public Task Add(string key, int delta)
        => state.PerformUpdate(s => { s.Items.TryGetValue(key, out var cur); s.Items[key] = cur + delta; });

    public Task Remove(string key)
        => state.PerformUpdate(s => s.Items.Remove(key));

    public Task RemoveMultiple(List<string> keys)
        => state.PerformUpdate(s => { foreach (var k in keys) s.Items.Remove(k); });

    public async Task SetMultipleAndThrow(Dictionary<string, int> items)
    {
        await SetMultiple(items);
        throw new PTestException("abort after set");
    }

    public async Task RemoveMultipleAndThrow(List<string> keys)
    {
        await RemoveMultiple(keys);
        throw new PTestException("abort after remove");
    }
}

[Serializable, GenerateSerializer]
public class PTestException : Exception
{
    public PTestException() { }
    public PTestException(string message) : base(message) { }
    public PTestException(string message, Exception inner) : base(message, inner) { }
    [Obsolete] protected PTestException(SerializationInfo info, StreamingContext ctx) : base(info, ctx) { }
}
