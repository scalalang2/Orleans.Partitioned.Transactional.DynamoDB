using Microsoft.Extensions.DependencyInjection;
using Orleans.TestingHost;
using Orleans.Transactions.TestKit;

namespace Orleans.Partitioned.Transactional.DynamoDB.Tests.PartitionedStateTest;

public class PTestFixture : IAsyncLifetime
{
    private TestCluster? _cluster;
    public TestCluster Cluster => _cluster ?? throw new InvalidOperationException("Cluster not initialized");
    public IGrainFactory GrainFactory => Cluster.GrainFactory;
    
    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
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
    
    public class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder hostBuilder)
        {
            hostBuilder
                .ConfigureServices(services =>
                    services.AddKeyedSingleton<IRemoteCommitService, RemoteCommitService>(
                        TransactionTestConstants.RemoteCommitService))
                .AddDynamoDBPartitionedTransactionalStateStorage("PartitionedTransactionStore", options =>
                {
                    options.Service = "http://localhost:8000";
                    options.AccessKey = "fake";
                    options.SecretKey = "fake";
                    options.TableName = "OrleansPartitionedTransactionTest";
                    options.UseProvisionedThroughput = false;
                    options.CreateIfNotExists = true;
                })
                .UseTransactions();
        }
    }
}