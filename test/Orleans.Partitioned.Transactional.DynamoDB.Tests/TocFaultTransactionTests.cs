using Orleans.Transactions.TestKit.xUnit;
using Xunit.Abstractions;

namespace Orleans.Partitioned.Transactional.DynamoDB.Tests;

[Trait("Category", "DynamoDB"), Trait("Category", "Transactions"), Trait("Category", "Functional")]
public class TocFaultTransactionTests : TocFaultTransactionTestRunnerxUnit, IClassFixture<TestFixture>
{
    public TocFaultTransactionTests(TestFixture fixture, ITestOutputHelper output)
        : base(fixture.GrainFactory, output)
    {
        fixture.EnsurePreconditionsMet();
    }
}
