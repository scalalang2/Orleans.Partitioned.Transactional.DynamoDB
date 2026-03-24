## Orleans.Partitioned.Transactional.DynamoDB

### Configuration

```csharp
builder.UseOrleans(silo =>
{
    silo.UseLocalhostClustering()
        .AddDynamoDBTransactionalStateStorageByDefault(options =>
        {
            options.Service = "ap-northeast-2";
            options.TableName = "OrleansTransactionState";
            options.UseProvisionedThroughput = false;
            options.CreateIfNotExists = true;
        })
        .AddDynamoDBPartitionedTransactionalStateStorage("PartitionedStorage", options =>
        {
            options.Service = "http://localhost:8000";
            options.AccessKey = "fake";
            options.SecretKey = "fake";

            // 테이블 이름이 동일해도 서로 간섭하지 않음
            options.TableName = "OrleansTransactionState";
            options.UseProvisionedThroughput = false;
            options.CreateIfNotExists = true;
        })
        .UseTransactions();
});
```

### Usage
```cs
[GenerateSerializer]
public class InventoryState : IPartitionedState<string, InventoryItem>
{
    [Id(0)]
    public int PartitionSize { get; set; } = 16;

    [Id(1)]
    public PartitionManifest Manifest { get; set; } = new();

    [Id(2)]
    public SortedDictionary<string, InventoryItem> Items { get; set; } = new();

    [Id(3)]
    public string WarehouseName { get; set; } = string.Empty;
}

```

The Grain implementation follows the standard Orleans transactional pattern.

```cs
public class InventoryGrain(
    [TransactionalState("inventory", "PartitionedStorage")]
    ITransactionalState<InventoryState> state) : Grain
{
    public Task SetItem(string itemId, InventoryItem item) =>
        state.PerformUpdate(s => s.Items[itemId] = item);

    public Task<InventoryItem?> GetItem(string itemId) =>
        state.PerformRead(s => s.Items.TryGetValue(itemId, out var item) ? item : null);
}

```