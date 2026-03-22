using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Partitioned.Transactional.DynamoDB.PartitionedTransactionalState;
using Orleans.Partitioned.Transactional.DynamoDB.TransactionalState;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.Transactions.Abstractions;

namespace Orleans.Hosting
{
    /// <summary>
    /// <see cref="IServiceCollection"/> extensions.
    /// </summary>
    public static class DynamoDBTransactionServiceCollectionExtensions
    {
        // internal static IServiceCollection AddDynamoDBTransactionalStateStorage(this IServiceCollection services,
        //     string name,
        //     Action<OptionsBuilder<DynamoDBTransactionalStorageOptions>> configureOptions = null)
        // {
        //     configureOptions?.Invoke(services.AddOptions<DynamoDBTransactionalStorageOptions>(name));
        //     services.AddTransient<IConfigurationValidator>(sp => new DynamoDBTransactionalStorageOptionsValidator(sp.GetRequiredService<IOptionsMonitor<DynamoDBTransactionalStorageOptions>>().Get(name), name));
        //     services.ConfigureNamedOptionForLogging<DynamoDBTransactionalStorageOptions>(name);
        //     services.AddTransient<IPostConfigureOptions<DynamoDBTransactionalStorageOptions>, DefaultStorageProviderSerializerOptionsConfigurator<DynamoDBTransactionalStorageOptions>>();
        //
        //     services.TryAddSingleton<ITransactionalStateStorageFactory>(sp => sp.GetKeyedService<ITransactionalStateStorageFactory>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME));
        //     services.AddKeyedSingleton<ITransactionalStateStorageFactory>(name, (sp, key) => DynamoDBTransactionalStateStorageFactory.Create(sp, key as string));
        //     services.AddSingleton<ILifecycleParticipant<ISiloLifecycle>>(s => (ILifecycleParticipant<ISiloLifecycle>)s.GetRequiredKeyedService<ITransactionalStateStorageFactory>(name));
        //
        //     return services;
        // }
        
        internal static IServiceCollection AddDynamoDBTransactionalStateStorage(
            this IServiceCollection services,
            string name,
            Action<OptionsBuilder<DynamoDBTransactionalStorageOptions>> configureOptions = null)
        {
            return services.AddDynamoDBTransactionalStateStorageInternal(
                name,
                configureOptions,
                DynamoDBTransactionalStateStorageFactory.Create);
        }

        internal static IServiceCollection AddDynamoDBPartitionedTransactionalStateStorage(
            this IServiceCollection services,
            string name,
            Action<OptionsBuilder<DynamoDBTransactionalStorageOptions>> configureOptions = null)
        {
            return services.AddDynamoDBTransactionalStateStorageInternal(
                name,
                configureOptions,
                DynamoDBPartitionedTransactionalStateStorageFactory.Create);
        }
        
        private static IServiceCollection AddDynamoDBTransactionalStateStorageInternal(
            this IServiceCollection services,
            string name,
            Action<OptionsBuilder<DynamoDBTransactionalStorageOptions>> configureOptions,
            Func<IServiceProvider, string, ITransactionalStateStorageFactory> factoryFactory)
        {
            configureOptions?.Invoke(services.AddOptions<DynamoDBTransactionalStorageOptions>(name));

            services.AddTransient<IConfigurationValidator>(sp => new DynamoDBTransactionalStorageOptionsValidator(sp.GetRequiredService<IOptionsMonitor<DynamoDBTransactionalStorageOptions>>().Get(name), name));
            services.ConfigureNamedOptionForLogging<DynamoDBTransactionalStorageOptions>(name);
            services.AddTransient<IPostConfigureOptions<DynamoDBTransactionalStorageOptions>, DefaultStorageProviderSerializerOptionsConfigurator<DynamoDBTransactionalStorageOptions>>();

            services.TryAddSingleton<ITransactionalStateStorageFactory>(sp => sp.GetKeyedService<ITransactionalStateStorageFactory>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME));
            services.AddKeyedSingleton<ITransactionalStateStorageFactory>(name, (sp, key) => factoryFactory(sp, key as string ?? name));
            services.AddSingleton<ILifecycleParticipant<ISiloLifecycle>>(sp => (ILifecycleParticipant<ISiloLifecycle>)sp.GetRequiredKeyedService<ITransactionalStateStorageFactory>(name));

            return services;
        }
    }

}