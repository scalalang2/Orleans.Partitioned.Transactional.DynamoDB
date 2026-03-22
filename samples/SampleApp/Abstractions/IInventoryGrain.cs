namespace SampleApp.Abstractions;

[GenerateSerializer]
public record InventoryItem
{
    [Id(0)] public string Name { get; set; } = string.Empty;
    [Id(1)] public int Quantity { get; set; }
    [Id(2)] public decimal Price { get; set; }
}

public interface IInventoryGrain : IGrainWithStringKey
{
    [Transaction(TransactionOption.CreateOrJoin)]
    Task SetItem(string itemId, InventoryItem item);

    [Transaction(TransactionOption.CreateOrJoin)]
    Task DeleteItem(string itemId);

    [Transaction(TransactionOption.CreateOrJoin)]
    Task<InventoryItem?> GetItem(string itemId);

    [Transaction(TransactionOption.CreateOrJoin)]
    Task<Dictionary<string, InventoryItem>> GetAllItems();

    [Transaction(TransactionOption.CreateOrJoin)]
    Task SetItems(Dictionary<string, InventoryItem> items);

    [Transaction(TransactionOption.CreateOrJoin)]
    Task ResetState(int partitionSize, Dictionary<string, InventoryItem> items);

    [Transaction(TransactionOption.CreateOrJoin)]
    Task BatchSetItems(Dictionary<string, InventoryItem> items);
}
