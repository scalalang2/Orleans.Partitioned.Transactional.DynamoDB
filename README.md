# Orleans.Partitioned.Transational.DynamoDB

## Overview
This package provides an Orleans transactional state storage implementation backed by DynamoDB.

It also supports **optional partitioned state** for large state payloads.

## Models

### `IPartitionedState<TPartition>`

A state type opts into partitioned state by implementing this interface.

The interface includes following fields..

- `PartitionSize`
- `PartitionVersion`
- `HashCodes`
- `Dictionary<uint partition, uint commmit sequence>`

## DynamoDB Storage Layout
A non-partitioned state uses following scheme.
  - `PartitionKey = grain specific partition key`
  - `RowKey = state_<commit_sequence | 0000000000XXX>`
  - `GrainState <serialized full state>`

A partitioned state store its value as below, It consists of `header` and `partition` rows
- Header
  - `PartitionKey = grain specific partition key`
  - `RowKey = state_<commit_sequence | 0000000000XXX>`
  - `GrainState = <serialized PartitionStateManifest>`
    - PartitionStateManifest includes `an array` which holds a tuple for two value.
      - `long commit sequence` indicates that what sequenced state should be used.
      - `long partition` indicates which partition should be loaded.

### Example of the storage layout
```
[BASE PK]
inventory/warehouse_default_inventory
    |
    +-- key -------------------------------> "latest committed seq = 3"
    |
    +-- state_0001 ------------------------> state v1 (old state will be deleted)
    +-- state_0002 ------------------------> state v2 (old state will be deleted)
    +-- state_0003 ------------------------> state v3
                                             manifest:
                                                p1 -> v3
                                                p2 -> v2
                                                p5 -> v3
                                                p6 -> v1

[PARTITION PKs]
inventory/warehouse_default_inventory_p1
    +-- state_0002 (old state will be deleted)
    +-- state_0003

inventory/warehouse_default_inventory_p2
    +-- state_0002

inventory/warehouse_default_inventory_p5
    +-- state_0003

inventory/warehouse_default_inventory_p6
    +-- state_0001
```
  
## Storage Engine
- The Storage Engine must prune all orphan partition data. e.g. all partitions lower committed sequence in the partition manifest of committed state

## Example

```csharp
// PLEASE DON'T EACH FIELD FOR THIS CLASS MANUALLY.
public class PartitionStateMetadata 
{
    // HashCodes is used to determine which partition is changed.
    // Storage engine only partially updates the parititon which should be updated.
    public Dictionary<uint, string> HashCodes;
    
    public Dictionary<uint, uint> PartitionToCommitSeq;
}

public class InventoryState : IPartitionedState<uint, ItemObject> 
{
    public int PartitionSize { get; set; } = 10;
    
    // if partition version changed, 
    // all partitions under storage layout moved to proper parittions
    public int PartitionVersion { get; set; } = 1;
    
    // Actual data we want to handle logically.
    public Dictionary<uint, ItemObject> Items;
}

public class InventoryGrain : Grain, IInventoryGrain 
{
    private readonly ITransactionalState<InventoryState> _state;
    
    public InventoryGrain([TransactionalState("Inventory")] ITransactionalState<InventoryState> state) 
    {
        this.state = _state;
    }
    
    ...
}
```

## Package
```sh
$ dotnet pack -c Release -p:PackageVersion=10.0.0
```