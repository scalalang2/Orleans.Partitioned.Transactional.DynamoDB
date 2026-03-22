using Orleans.Transactions.Abstractions;
using SampleApp.Abstractions;

namespace SampleApp.Grains;

public class InventoryGrain(
    [TransactionalState("inventory", "PartitionedStorage")]
    ITransactionalState<InventoryState> state) : Grain, IInventoryGrain
{
    public async Task SetItem(string itemId, InventoryItem item)
    {
        await state.PerformUpdate(s => s.Items[itemId] = item);
    }

    public async Task DeleteItem(string itemId)
    {
        await state.PerformUpdate(s => s.Items.Remove(itemId));
    }

    public async Task<InventoryItem?> GetItem(string itemId)
    {
        return await state.PerformRead(s => s.Items.TryGetValue(itemId, out var item) ? item : null);
    }

    public async Task<Dictionary<string, InventoryItem>> GetAllItems()
    {
        return await state.PerformRead(s => new Dictionary<string, InventoryItem>(s.Items));
    }

    public async Task SetItems(Dictionary<string, InventoryItem> items)
    {
        await state.PerformUpdate(s =>
        {
            foreach (var (key, value) in items)
            {
                s.Items[key] = value;
            }
        });
    }

    public async Task ResetState(int partitionSize, Dictionary<string, InventoryItem> items)
    {
        await state.PerformUpdate(s =>
        {
            s.PartitionSize = partitionSize;
            s.Manifest = new();
            s.Items = new SortedDictionary<string, InventoryItem>(items);
        });
    }

    public async Task BatchSetItems(Dictionary<string, InventoryItem> items)
    {
        await state.PerformUpdate(s =>
        {
            foreach (var (key, value) in items)
            {
                s.Items[key] = value;
            }
        });
    }
}
