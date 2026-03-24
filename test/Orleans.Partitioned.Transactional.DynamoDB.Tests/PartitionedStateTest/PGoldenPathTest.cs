using Orleans.Partitioned.Transactional.DynamoDB.Tests.PartitionedStateTest.Grains;
using Orleans.Transactions;
using Shouldly;
using Xunit.Abstractions;

namespace Orleans.Partitioned.Transactional.DynamoDB.Tests.PartitionedStateTest;

[Trait("Category", "DynamoDB"), Trait("Category", "Transactions"), Trait("Category", "Functional")]
public class PGoldenPathTest(PTestFixture fixture, ITestOutputHelper output) : IClassFixture<PTestFixture>
{
    private readonly IGrainFactory _gf = fixture.GrainFactory;
    
    private ITestGrain NewGrain() => _gf.GetGrain<ITestGrain>(Guid.NewGuid());
    
    private static Dictionary<string, int> Items(int count, string prefix = "k")
        => Enumerable.Range(0, count).ToDictionary(i => $"{prefix}-{i:D3}", i => i);

    [Fact]
    public async Task Read_Empty_ReturnsNull()
    {
        var g = NewGrain();
        (await g.Get("nope")).ShouldBeNull();
        (await g.GetAll()).ShouldBeEmpty();
    }

    [Fact]
    public async Task Set_Get_SingleKey()
    {
        var g = NewGrain();
        await g.Set("k1", 100);
        
        (await g.Get("k1")).ShouldBe(100);
        var all = await g.GetAll();
        all.Count.ShouldBe(1);
        all["k1"].ShouldBe(100);
    }

    [Fact]
    public async Task SetMultiple_Get_MultipleKeys()
    {
        var g = NewGrain();
        var items = Items(10);
        await g.SetMultiple(items);

        var all = await g.GetAll();
        all.Count.ShouldBe(10);
        foreach (var (k, v) in items)
        {
            all[k].ShouldBe(v);
        }
    }

    [Fact]
    public async Task Update_ExistingKey()
    {
        var g = NewGrain();
        await g.Set("k1", 100);
        await g.Add("k1", 50);

        (await g.Get("k1")).ShouldBe(150);
    }

    [Fact]
    public async Task Remove_Key()
    {
        var g = NewGrain();
        await g.Set("k1", 100);
        await g.Remove("k1");

        (await g.Get("k1")).ShouldBeNull();
        (await g.GetAll()).ShouldBeEmpty();
    }

    [Fact]
    public async Task Transaction_Abort_RollsBackState()
    {
        var g = NewGrain();
        await g.Set("initial", 1);

        var items = Items(5, "fail");
        await Should.ThrowAsync<OrleansTransactionAbortedException>(() => g.SetMultipleAndThrow(items));

        // Should only have initial state
        var all = await g.GetAll();
        all.Count.ShouldBe(1);
        all.ShouldContainKey("initial");
        all.ShouldNotContainKey("fail-000");
    }
    
    [Theory]
    [InlineData(3)]   // 1 partition
    [InlineData(9)]   // 3 partitions
    [InlineData(30)]  // 10 partitions
    [InlineData(50)]  // 17 partitions
    public async Task SetMultiple_GetAll_VerifyEntireState(int count)
    {
        var g = NewGrain();
        var expected = Items(count);
        await g.SetMultiple(expected);

        var actual = await g.GetAll();
        expected.Count.ShouldBe(actual.Count);
        foreach (var (k, v) in expected)
            v.ShouldBe(actual[k]);
    }

    [Fact]
    public async Task IncrementalWrites_ThenVerifyAll()
    {
        var g = NewGrain();
        var batch1 = Items(5, "a");
        var batch2 = Items(5, "b");

        await g.SetMultiple(batch1);
        await g.SetMultiple(batch2);

        var all = await g.GetAll();
        all.Count.ShouldBe(10);
        foreach (var (k, v) in batch1) all[k].ShouldBe(v);
        foreach (var (k, v) in batch2) all[k].ShouldBe(v);
    }
}