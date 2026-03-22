using Amazon.DynamoDBv2.Model;
using Orleans.Partitioned.Transactional.DynamoDB.Internal;
using Orleans.Partitioned.Transactional.DynamoDB.TransactionalState;

namespace Orleans.Partitioned.Transactional.DynamoDB.Tests;

public class StorageEntityTests
{
    [Fact]
    public void MakeRowKey_UsesFixedWidthHexFormat()
    {
        var rowKey = StateEntity.MakeRowKey(255);

        Assert.Equal("state_00000000000000ff", rowKey);
    }

    [Fact]
    public void SequenceId_RoundTripsFromRowKey()
    {
        var entity = new StateEntity
        {
            PartitionKey = "partition",
            RowKey = StateEntity.MakeRowKey(42)
        };

        Assert.Equal(42, entity.SequenceId);
    }

    [Fact]
    public void KeyEntity_RoundTripsStorageFields()
    {
        var expectedTimestamp = DateTimeOffset.UtcNow;
        var expectedMetadata = new byte[] { 1, 2, 3 };

        var entity = new KeyEntity("partition")
        {
            CommittedSequenceId = 7,
            Metadata = expectedMetadata,
            Timestamp = expectedTimestamp,
            ETag = 11
        };

        var rehydrated = new KeyEntity(entity.ToStorageFormat());

        Assert.Equal("partition", rehydrated.PartitionKey);
        Assert.Equal(KeyEntity.RK, rehydrated.RowKey);
        Assert.Equal(7, rehydrated.CommittedSequenceId);
        Assert.Equal(expectedMetadata, rehydrated.Metadata);
        Assert.Equal(expectedTimestamp.ToUnixTimeSeconds(), rehydrated.Timestamp.ToUnixTimeSeconds());
        Assert.Equal(11, rehydrated.ETag);
    }

    [Fact]
    public void StateEntity_ToStorageFormat_OnlyIncludesOptionalFieldsWhenPresent()
    {
        var entity = new StateEntity
        {
            PartitionKey = "partition",
            RowKey = StateEntity.MakeRowKey(9),
            TransactionId = "tx-1",
            TransactionTimestamp = new DateTime(2026, 3, 22, 0, 0, 0, DateTimeKind.Utc),
            TransactionManager = new byte[] { 4, 5, 6 },
            State = new byte[] { 7, 8, 9 },
            ETag = 3
        };

        Dictionary<string, AttributeValue> item = entity.ToStorageFormat();

        Assert.Equal("partition", item[DynamoDBTransactionalStateConstants.PARTITION_KEY_PROPERTY_NAME].S);
        Assert.Equal(entity.RowKey, item[DynamoDBTransactionalStateConstants.ROW_KEY_PROPERTY_NAME].S);
        Assert.Equal("tx-1", item[StateEntity.TRANSACTION_ID_PROPERTY_NAME].S);
        Assert.Equal("2026-03-22T00:00:00.0000000Z", item[StateEntity.TRANSACTION_TIMESTAMP_PROPERTY_NAME].S);
        Assert.Equal(new byte[] { 4, 5, 6 }, item[StateEntity.TRANSACTION_MANAGER_PROPERTY_NAME].B.ToArray());
        Assert.Equal(new byte[] { 7, 8, 9 }, item[DynamoDBTransactionalStateConstants.BINARY_STATE_PROPERTY_NAME].B.ToArray());
        Assert.Equal("3", item[DynamoDBTransactionalStateConstants.ETAG_PROPERTY_NAME].N);
    }

    [Fact]
    public void ValidateDynamoDbPartitionKey_RejectsTooLongKeys()
    {
        var key = new string('a', 2048);

        var exception = Assert.Throws<ArgumentException>(() => AWSUtils.ValidateDynamoDBPartitionKey(key));

        Assert.Contains("too long", exception.Message);
    }

    [Fact]
    public void ValidateDynamoDbRowKey_RejectsTooLongKeys()
    {
        var key = new string('b', 1024);

        var exception = Assert.Throws<ArgumentException>(() => AWSUtils.ValidateDynamoDBRowKey(key));

        Assert.Contains("too long", exception.Message);
    }
}
