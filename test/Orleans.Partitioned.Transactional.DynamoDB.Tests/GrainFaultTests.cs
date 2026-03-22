using Orleans.Transactions.TestKit.xUnit;
using Xunit.Abstractions;

namespace Orleans.Partitioned.Transactional.DynamoDB.Tests;

[Trait("Category", "DynamoDB"), Trait("Category", "Transactions"), Trait("Category", "Functional")]
public class GrainFaultTests : GrainFaultTransactionTestRunnerxUnit, IClassFixture<TestFixture>
{
    public GrainFaultTests(TestFixture fixture, ITestOutputHelper output)
        : base(fixture.GrainFactory, output)
    {
        fixture.EnsurePreconditionsMet();
    }
}
