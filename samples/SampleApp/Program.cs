using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Order;
using BenchmarkDotNet.Running;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SampleApp.Abstractions;

const string benchmarkMode = "benchmark";

if (args.Length > 0 && string.Equals(args[0], benchmarkMode, StringComparison.OrdinalIgnoreCase))
{
    BenchmarkRunner.Run<PartitionStorageBenchmarks>(new PartitionBenchmarkConfig());
    return;
}

using var host = await BenchmarkHost.CreateAsync();
await RunSampleAsync(host.Services);

static async Task RunSampleAsync(IServiceProvider services)
{
    var grainFactory = services.GetRequiredService<IGrainFactory>();
    var inventoryGrain = grainFactory.GetGrain<IInventoryGrain>("warehouse");

    Console.WriteLine(">>> Fetch stored items");
    var initialItems = await inventoryGrain.GetAllItems();
    Console.WriteLine($">>> Current item count: {initialItems.Count}");
    foreach (var item in initialItems)
    {
        Console.WriteLine($"Item: {item.Key} | Name: {item.Value.Name} | Qty: {item.Value.Quantity} | Price: {item.Value.Price:C}");
    }

    var sampleItems = BenchmarkData.CreateInventoryItems(20, 0);
    Console.WriteLine(">>> Initializing inventory with 20 items...");
    await inventoryGrain.SetItems(sampleItems);

    Console.WriteLine(">>> Fetching all items from partitioned storage...");
    var allItems = await inventoryGrain.GetAllItems();
    foreach (var item in allItems.OrderBy(x => x.Key))
    {
        Console.WriteLine($"Item: {item.Key} | Name: {item.Value.Name} | Qty: {item.Value.Quantity} | Price: {item.Value.Price:C}");
    }

    Console.WriteLine("\n>>> Updating item-00000 quantity...");
    var firstItem = await inventoryGrain.GetItem("item-00000");
    if (firstItem != null)
    {
        firstItem.Quantity -= 1;
        await inventoryGrain.SetItem("item-00000", firstItem);
        Console.WriteLine($"New item-00000 Quantity: {firstItem.Quantity}");
    }

    var finalItems = await inventoryGrain.GetAllItems();
    Console.WriteLine($">>> Final item count: {finalItems.Count}");
}

[MemoryDiagnoser]
[HideColumns(Column.Job, Column.Error, Column.StdDev, Column.RatioSD)]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[Config(typeof(PartitionBenchmarkConfig))]
public class PartitionStorageBenchmarks
{
    private const int SeedItemCount = 10_000;
    private const int ReadOperationsPerInvoke = 5;
    private const int SingleWriteOperationsPerInvoke = 20;
    private const int BatchWriteOperationsPerInvoke = 10;
    private const int BatchSize = 100;

    private static readonly SemaphoreSlim SetupLock = new(1, 1);
    private static readonly string GrainKey = "benchmark-warehouse";
    private static readonly Dictionary<string, InventoryItem> SeedItems = BenchmarkData.CreateInventoryItems(SeedItemCount, 0);
    private static BenchmarkHost? sharedHost;

    private IInventoryGrain grain = default!;
    private int singleWriteCursor;
    private int batchWriteCursor;

    [ParamsSource(nameof(PartitionSizes))]
    public int PartitionSize { get; set; }

    public IEnumerable<int> PartitionSizes() => Enumerable.Range(1, 100);

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        await SetupLock.WaitAsync();
        try
        {
            sharedHost ??= await BenchmarkHost.CreateAsync();
            grain = sharedHost.GrainFactory.GetGrain<IInventoryGrain>(GrainKey);
            singleWriteCursor = 0;
            batchWriteCursor = 0;
            await grain.ResetState(PartitionSize, SeedItems);

            var allItems = await grain.GetAllItems();
            if (allItems.Count != SeedItemCount)
            {
                throw new InvalidOperationException($"Seed validation failed for partition size {PartitionSize}. Expected {SeedItemCount}, got {allItems.Count}.");
            }
        }
        finally
        {
            SetupLock.Release();
        }
    }

    [Benchmark(Description = "Read", OperationsPerInvoke = ReadOperationsPerInvoke)]
    public async Task<int> ReadAsync()
    {
        var total = 0;
        for (var i = 0; i < ReadOperationsPerInvoke; i++)
        {
            total += (await grain.GetAllItems()).Count;
        }

        return total;
    }

    [Benchmark(Description = "SingleWrite", OperationsPerInvoke = SingleWriteOperationsPerInvoke)]
    public async Task SingleWriteAsync()
    {
        for (var i = 0; i < SingleWriteOperationsPerInvoke; i++)
        {
            var itemIndex = Interlocked.Increment(ref singleWriteCursor) - 1;
            var itemId = $"item-{itemIndex % SeedItemCount:D5}";
            await grain.SetItem(itemId, BenchmarkData.CreateInventoryItem(itemIndex + PartitionSize));
        }
    }

    [Benchmark(Description = "BatchWrite", OperationsPerInvoke = BatchWriteOperationsPerInvoke)]
    public async Task BatchWriteAsync()
    {
        for (var batchIndex = 0; batchIndex < BatchWriteOperationsPerInvoke; batchIndex++)
        {
            var cursor = Interlocked.Add(ref batchWriteCursor, BatchSize) - BatchSize;
            var batch = new Dictionary<string, InventoryItem>(BatchSize);
            for (var offset = 0; offset < BatchSize; offset++)
            {
                var itemIndex = (cursor + offset) % SeedItemCount;
                batch[$"item-{itemIndex:D5}"] = BenchmarkData.CreateInventoryItem(cursor + offset + PartitionSize);
            }

            await grain.BatchSetItems(batch);
        }
    }
}

internal static class BenchmarkData
{
    public static Dictionary<string, InventoryItem> CreateInventoryItems(int count, int seed)
    {
        var items = new Dictionary<string, InventoryItem>(count);
        for (var i = 0; i < count; i++)
        {
            items[$"item-{i:D5}"] = CreateInventoryItem(i + seed);
        }

        return items;
    }

    public static InventoryItem CreateInventoryItem(int index)
    {
        var quantity = (index % 100) + 1;
        var price = decimal.Round(10 + (index % 500) * 0.75m, 2);
        return new InventoryItem
        {
            Name = $"Product-{index:D5}",
            Quantity = quantity,
            Price = price
        };
    }
}

internal sealed class PartitionBenchmarkConfig : ManualConfig
{
    public PartitionBenchmarkConfig()
    {
        AddJob(Job.Default
            .WithId("PartitionSweep")
            .WithLaunchCount(1)
            .WithWarmupCount(3)
            .WithIterationCount(12));

        AddExporter(MarkdownExporter.GitHub);
        AddExporter(CsvExporter.Default);
        AddColumnProvider(DefaultColumnProviders.Instance);
        AddColumn(StatisticColumn.Min, StatisticColumn.Max, StatisticColumn.Median, StatisticColumn.P95);
        ArtifactsPath = "benchmark-results";
    }
}

internal sealed class BenchmarkHost : IAsyncDisposable
{
    private readonly IHost host;

    private BenchmarkHost(IHost host)
    {
        this.host = host;
    }

    public IGrainFactory GrainFactory => this.host.Services.GetRequiredService<IGrainFactory>();

    public static async Task<BenchmarkHost> CreateAsync()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.AddConsole();

        builder.UseOrleans(siloBuilder =>
        {
            siloBuilder.UseLocalhostClustering()
                .AddDynamoDBPartitionedTransactionalStateStorage("PartitionedStorage", options =>
                {
                    options.Service = "http://localhost:8000";
                    options.AccessKey = "fake";
                    options.SecretKey = "fake";
                    options.TableName = "InventorySample";
                    options.UseProvisionedThroughput = false;
                    options.CreateIfNotExists = true;
                })
                .UseTransactions();
        });

        var host = builder.Build();
        await host.StartAsync();
        return new BenchmarkHost(host);
    }

    public async ValueTask DisposeAsync()
    {
        await this.host.StopAsync();
        this.host.Dispose();
    }
}
