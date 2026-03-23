using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans;
using SampleApp;
using SampleApp.Abstractions;

var builder = Host.CreateApplicationBuilder(args);
builder.Logging.AddConsole();
builder.Services.AddSampleTracing(builder.Configuration);
builder.UseOrleans(siloBuilder =>
{
    siloBuilder.UseLocalhostClustering()
        .AddActivityPropagation()
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

using var sampleActivity = SampleTelemetry.ActivitySource.StartActivity("sample.inventory-scenario");

var grainFactory = host.Services.GetRequiredService<IGrainFactory>();
var inventoryGrain = grainFactory.GetGrain<IInventoryGrain>("warehouse");

Console.WriteLine(">>> Fetch stored items");
var initialItems = await inventoryGrain.GetAllItems();
Console.WriteLine($">>> Current item count: {initialItems.Count}");
foreach (var item in initialItems)
{
    Console.WriteLine($"Item: {item.Key} | Name: {item.Value.Name} | Qty: {item.Value.Quantity} | Price: {item.Value.Price:C}");
}

var sampleItems = new Dictionary<string, InventoryItem>();
var products = new (string Id, string Name, int Qty, decimal Price)[]
{
    ("P001", "Laptop", 10, 1200.00m),
    ("P002", "Smartphone", 50, 800.00m),
    ("P003", "Monitor", 25, 300.00m),
    ("P004", "Keyboard", 100, 50.00m),
    ("P005", "Mouse", 120, 25.00m),
    ("P006", "Headphones", 40, 150.00m),
    ("P007", "Webcam", 30, 80.00m),
    ("P008", "Microphone", 15, 200.00m),
    ("P009", "External SSD", 60, 120.00m),
    ("P010", "USB-C Hub", 80, 45.00m),
    ("P011", "Graphics Card", 5, 700.00m),
    ("P012", "Power Supply", 20, 110.00m),
    ("P013", "RAM Kit 32GB", 35, 130.00m),
    ("P014", "CPU Cooler", 22, 65.00m),
    ("P015", "PC Case", 12, 90.00m),
    ("P016", "Motherboard", 18, 210.00m),
    ("P017", "Tablet", 14, 450.00m),
    ("P018", "Smartwatch", 28, 199.00m),
    ("P019", "Printer", 8, 250.00m),
    ("P020", "Router", 45, 125.00m)
};

foreach (var p in products)
{
    sampleItems.Add(p.Id, new InventoryItem { Name = p.Name, Quantity = p.Qty, Price = p.Price });
}

Console.WriteLine(">>> Initializing inventory with 20 items...");
await inventoryGrain.SetItems(sampleItems);

Console.WriteLine(">>> Fetching all items from partitioned storage...");
var allItems = await inventoryGrain.GetAllItems();
foreach (var item in allItems.OrderBy(x => x.Key))
{
    Console.WriteLine($"Item: {item.Key} | Name: {item.Value.Name} | Qty: {item.Value.Quantity} | Price: {item.Value.Price:C}");
}

Console.WriteLine("\n>>> Updating P001 (Laptop) quantity...");
var laptop = await inventoryGrain.GetItem("P001");
if (laptop != null)
{
    laptop.Quantity -= 1;
    await inventoryGrain.SetItem("P001", laptop);
    Console.WriteLine($"New P001 Quantity: {laptop.Quantity}");
}

Console.WriteLine("\n>>> Deleting P020 (Router)...");
await inventoryGrain.DeleteItem("P020");

var finalItems = await inventoryGrain.GetAllItems();
Console.WriteLine($">>> Final item count: {finalItems.Count}");

await host.StopAsync();
