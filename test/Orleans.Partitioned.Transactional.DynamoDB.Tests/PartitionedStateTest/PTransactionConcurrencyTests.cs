using Orleans.Partitioned.Transactional.DynamoDB.Tests.PartitionedStateTest.Grains;
using Orleans.Transactions;
using Shouldly;
using Xunit.Abstractions;

namespace Orleans.Partitioned.Transactional.DynamoDB.Tests.PartitionedStateTest;

[Trait("Category", "DynamoDB"), Trait("Category", "Transactions"), Trait("Category", "Functional")]
public class PTransactionConcurrencyTests(PTestFixture fixture, ITestOutputHelper output) : IClassFixture<PTestFixture>
{
    private readonly IGrainFactory _gf = fixture.GrainFactory;
    
    private ITestGrain NewGrain() => _gf.GetGrain<ITestGrain>(Guid.NewGuid());
    
    private static Dictionary<string, int> Items(int count, string prefix = "k")
        => Enumerable.Range(0, count).ToDictionary(i => $"{prefix}-{i:D3}", i => i);
    
    private static void AssertState(Dictionary<string, int> expected, Dictionary<string, int> actual)
    {
        actual.Count.ShouldBe(expected.Count);
        foreach (var (k, v) in expected)
        {
            actual.ShouldContainKey(k);
            actual[k].ShouldBe(v);
        }
    }

    [Fact]
    public async Task ConcurrentBatchWrites_SharedGrain()
    {
        var g = NewGrain();
        var batch1 = Items(10, "batch1");
        var batch2 = Items(10, "batch2");

        // Run two transactions concurrently against the same grain
        await Task.WhenAll(
            g.SetMultiple(batch1),
            g.SetMultiple(batch2)
        );

        var all = await g.GetAll();
        all.Count.ShouldBe(20);
        foreach (var (k, v) in batch1) all[k].ShouldBe(v);
        foreach (var (k, v) in batch2) all[k].ShouldBe(v);
    }

    [Fact]
    public async Task MultiGrain_AtomicTransaction_Success()
    {
        var coordinator = _gf.GetGrain<ITestCoordinatorGrain>(Guid.NewGuid());
        var g1 = NewGrain();
        var g2 = NewGrain();
        var items = Items(50);

        await coordinator.MultiGrainSet([g1, g2], items);

        AssertState(items, await g1.GetAll());
        AssertState(items, await g2.GetAll());
    }

    [Fact]
    public async Task MultiGrain_AtomicTransaction_Rollback()
    {
        var coordinator = _gf.GetGrain<ITestCoordinatorGrain>(Guid.NewGuid());
        var gSuccess = NewGrain();
        var gFail = NewGrain();
        var items = Items(50);

        // One grain will throw, causing the entire distributed transaction to abort
        await Should.ThrowAsync<OrleansTransactionAbortedException>(() => 
            coordinator.MultiGrainSetAndThrow([gFail], [gSuccess], items));

        (await gSuccess.GetAll()).ShouldBeEmpty();
        (await gFail.GetAll()).ShouldBeEmpty();
    }
    
    [Fact]
    public async Task TransactionChain_AllGrainsConsistent()
    {
        var g1 = NewGrain();
        var g2 = NewGrain();
        var g3 = NewGrain();
        var g4 = NewGrain();

        var items = Items(9);

        var c1 = _gf.GetGrain<ITestCoordinatorGrain>(Guid.NewGuid());
        var c2 = _gf.GetGrain<ITestCoordinatorGrain>(Guid.NewGuid());
        var c3 = _gf.GetGrain<ITestCoordinatorGrain>(Guid.NewGuid());

        await Task.WhenAll(
            c1.MultiGrainSet([g1, g2], items),
            c2.MultiGrainSet([g2, g3], items),
            c3.MultiGrainSet([g3, g4], items));
        
        foreach (var g in new[] { g1, g2, g3, g4 })
            AssertState(items, await g.GetAll());
    }
    
    [Fact]
    public async Task ConcurrentMultiGrainBatches_AllConsistent()
    {
        const int grainCount = 5;
        var grains = Enumerable.Range(0, grainCount).Select(_ => NewGrain()).ToList();

        var batch1 = Items(6, "x");
        var batch2 = Items(6, "y");

        var c1 = _gf.GetGrain<ITestCoordinatorGrain>(Guid.NewGuid());
        var c2 = _gf.GetGrain<ITestCoordinatorGrain>(Guid.NewGuid());

        await c1.MultiGrainSet(grains, batch1);
        await c2.MultiGrainSet(grains, batch2);
        
        var expected = batch1.Concat(batch2).ToDictionary(kv => kv.Key, kv => kv.Value);
        foreach (var g in grains)
            AssertState(expected, await g.GetAll());
    }
}