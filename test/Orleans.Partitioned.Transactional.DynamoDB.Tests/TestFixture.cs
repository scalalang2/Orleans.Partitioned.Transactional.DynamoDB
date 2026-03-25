using Microsoft.Extensions.DependencyInjection;
using Orleans.TestingHost;
using Orleans.Transactions.TestKit;

namespace Orleans.Partitioned.Transactional.DynamoDB.Tests;

public class TestFixture : IAsyncLifetime
{
    private TestCluster? _cluster;

    public TestCluster Cluster => _cluster ?? throw new InvalidOperationException("Cluster not initialized");
    public IGrainFactory GrainFactory => Cluster.GrainFactory;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<SiloBuilderConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    public void EnsurePreconditionsMet() { }

    public class SiloBuilderConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder hostBuilder)
        {
            hostBuilder
                .ConfigureServices(services =>
                    services.AddKeyedSingleton<IRemoteCommitService, RemoteCommitService>(
                        TransactionTestConstants.RemoteCommitService))
                .AddDynamoDBTransactionalStateStorage(TransactionTestConstants.TransactionStore, options =>
                {
                    options.Service = "http://localhost:8000";
                    options.AccessKey = "fake";
                    options.SecretKey = "fake";
                    options.TableName = "OrleansTransactionTest";
                    options.UseProvisionedThroughput = false;
                    options.CreateIfNotExists = true;
                })
                .UseTransactions();
        }
    }
}
