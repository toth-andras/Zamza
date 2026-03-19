using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Zamza.Consumer.CustomProcessing;
using Zamza.Consumer.Models;
using Zamza.Consumer.Models.ConsumerMetadata;
using Zamza.Consumer.ServerFacade;

namespace Zamza.Consumer;

public static class ZamzaServiceCollectionExtensions
{
    public static IServiceCollection AddZamzaConsumer<TKey, TValue, TCustomProcessor>(
        this IServiceCollection services, ZamzaConsumerSettings<TKey, TValue> settings) where TCustomProcessor : class, ICustomProcessor<TKey, TValue>
    {
        ValidateOptions(settings.KafkaConsumerConfig);
        
        services.AddTransient<ICustomProcessor<TKey, TValue>, TCustomProcessor>();
        
        services.AddHostedService<ConsumptionController<TKey, TValue>>(sp =>
        {
            var metadata = new ConsumerMetadata<TKey, TValue>(
                Guid.CreateVersion7().ToString(),
                settings.ConsumerGroup,
                settings.BearerToken,
                [],
                new ConcurrentDictionary<(string Topic, int Partition), PartitionOwnership>(),
                new ConcurrentDictionary<(string Topic, int Partition), long>(),
                settings.MaxRetries,
                settings.MinRetriesGap,
                settings.ProcessingPeriod,
                settings.ZamzaCallPerKafkaCalls,
                settings.RetryGapEvaluator,
                settings.FetchLimit);
            
            var kafkaConsumer = new ConsumerBuilder<TKey, TValue>(settings.KafkaConsumerConfig)
                .SetPartitionsAssignedHandler((_, _) =>
                {
                    Console.WriteLine("============= Rebalance ===================");
                    metadata.MarkPartitionOwnershipUpdateAsRequired();
                })
                .SetPartitionsRevokedHandler((_, _) =>
                {
                    Console.WriteLine("============= Rebalance ===================");
                    metadata.MarkPartitionOwnershipUpdateAsRequired();
                })
                .SetPartitionsLostHandler((_, _) =>
                {
                    Console.WriteLine("============= Rebalance ===================");
                    metadata.MarkPartitionOwnershipUpdateAsRequired();
                })
                .Build();
            
            kafkaConsumer.Subscribe(settings.Topics);
            
            var zamzaServerFacade = new ZamzaServerFacade<TKey, TValue>(settings.ZamzaServerUri);
            
            var customProcessor = sp.GetRequiredService<ICustomProcessor<TKey, TValue>>();
            var logger = sp.GetRequiredService<ILogger<ConsumptionController<TKey, TValue>>>();

            return new ConsumptionController<TKey, TValue>(
                kafkaConsumer,
                zamzaServerFacade,
                metadata,
                customProcessor,
                logger);
        });

        return services;
    }

    private static void ValidateOptions(ConsumerConfig config)
    {
        if (config.EnableAutoCommit is true)
        {
            throw new ArgumentException("enable.auto.commit must be false");
        }
    }

    private static void SetupRequiredOptions(ConsumerConfig config)
    {
        config.EnableAutoCommit = false;
    }
}