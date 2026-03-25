# 개요

해당 프로젝트는 Orleans에서 DynamoDB의 트랜잭션 프로바이더를 제공합니다.

이 레포지토리는 아래 2가지 모드를 지원합니다.

- `AddDynamoDBTransactionalStateStorage` : 기본 트랜잭션 상태 모드
- `AddDynamoDBPartitionedTransactionalStateStorage` : 대량의 데이터를 `Dictionary` 상태로 관리하는 스토리지 모드

파티셔닝 상태를 사용하려면 반드시 알맞은 스토리지를 지정해야 하고, 상태 클래스는 `IPartitionedState<K,V>` 인터페이스를 구현해야 합니다.

## Feature
- 파티셔닝 상태
    - Orleans 트랜잭션 상태와 동일한 수준의 트랜잭션을 보장합니다.
    - 파티션 크기 변경에 따른 라이브 파티션 리밸런싱을 지언합니다.
    - 변경된 파티션만 부분적으로 업데이트 합니다

## 설정

```cs
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

## 샘플 코드

파티션 스토리지를 이용한다면, 상태는 반드시 `IPartitionedState<K,V>` 인터페이스를 구현해야 합니다.

이 때, `PartitionSize`는 자유롭게 수정해도 되지만, `Manifest`는 객체는 직접 수정하면 안됩니다 

> TODO: Manifest를 DynamoDB 추가 레코드를 선언하고 저장해서 인터페이스에서 숨기기

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

Grain 코드에서는 일반적으로 하는 것과 동일한 코드를 작성하면 됩니다.

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

## 스토리지 레이아웃
기본적으로 Grain/State 쌍은 아래 파티션 키로 DynamoDB에 저장됩니다.
- {grainId}_{serviceId}_{stateName}

동일한 파티션 키에 대해서, 아래 2개의 RowKey를 가진 레코드가 생성됩니다.
- `RowKey = key` : 커밋 시퀀스, 트랜잭션 메타데이터, 타임스탬프가 저장되며 스토리지 엔진이 어떤 상태 데이터가 커밋된 상태인지 결정짓는 용도로 사용합니다.
- `RowKey = state_{sequenceId:x16}` : 실제 저장되는 상태이며 커밋된 상태, Prepared된 상태, Aborted된 상태 등 트랜잭션 과정의 중간 상태 들이 저장됩니다. `key` 레코드에 의해 최종적으로 커밋된 상태만 남고 다른 모든 레코드는 삭제됩니다.

파티션 모드에서는 이에 더불어 레코드가 더 저장되는데요.

- `PartitionKey = {partitionKey}_p{partitionNumber}` : DynamoDB는 파티션 키가 동일한 채 레코드만 분할 한다고 성능 효과가 올라가진 않습니다. 파티션 키를 다르게 설정해야 의미가 있기 때문에 각 파티션들은 `_p{파티션 번호}`의 접미사를 붙여서 생성합니다.
    - 파티션 또한 트랜잭션 중간 상태에 따라 `Prepared`, `Committed`, `Aborted` 등 다양한 상태의 레코드가 생성되는데요. 스토리지 엔진에서는 실제로 변경이 일어난 파티션에 대해서만 레코드가 생성되기 때문에 효율적으로 동작합니다.

```sh
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