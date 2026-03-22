using Orleans.Transactions.TestKit.xUnit;
using Xunit.Abstractions;

namespace Orleans.Partitioned.Transactional.DynamoDB.Tests;

[Trait("Category", "DynamoDB"), Trait("Category", "Transactions")]
public class ConsistencyTests : ConsistencyTransactionTestRunnerxUnit, IClassFixture<TestFixture>
{
    public ConsistencyTests(TestFixture fixture, ITestOutputHelper output)
        : base(fixture.GrainFactory, output)
    {
        fixture.EnsurePreconditionsMet();
    }

    protected override bool StorageAdaptorHasLimitedCommitSpace => true;
    protected override bool StorageErrorInjectionActive => false;
}
