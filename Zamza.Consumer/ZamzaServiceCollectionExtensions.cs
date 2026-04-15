using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Zamza.Consumer.Internal.BackgroundTask;
using Zamza.Consumer.Internal.ConsumptionController;
using Zamza.Consumer.Internal.MessageProcessing;
using Zamza.Consumer.Internal.RebalanceListener;
using Zamza.Consumer.Internal.Utils.DateTimeProvider;
using Zamza.Consumer.Internal.ZamzaServer;

namespace Zamza.Consumer;

public static class ZamzaServiceCollectionExtensions
{
    public static IServiceCollection AddZamzaConsumer<TKey, TValue, TMessageProcessor>(
        this IServiceCollection services,
        IEnumerable<string> topics,
        ZamzaConsumerSettings<TKey, TValue> config) where TMessageProcessor : IMessageCustomProcessor<TKey, TValue>
    {
        ArgumentNullException.ThrowIfNull(topics);
        ArgumentNullException.ThrowIfNull(config);
        
        services.TryAddSingleton<IDateTimeProvider, DateTimeProvider>();

        services.AddHostedService<ZamzaConsumerBackgroundTask<TKey, TValue>>(sp =>
        {
            var dateTimeProvider = sp.GetRequiredService<IDateTimeProvider>();

            var kafkaRebalanceListener = new KafkaConsumerGroupRebalanceListener<TKey, TValue>();
            
            var kafkaConsumer = new ConsumerBuilder<TKey, TValue>(config.KafkaConsumerConfig)
                .SetPartitionsAssignedHandler((_, _) => { kafkaRebalanceListener.OnRebalance(); })
                .SetPartitionsRevokedHandler((_, _) => { kafkaRebalanceListener.OnRebalance(); })
                .SetPartitionsLostHandler((_, _) => { kafkaRebalanceListener.OnRebalance(); })
                .Build();
        
            kafkaConsumer.Subscribe(topics);

            var zamzaServerFacade = new ZamzaServerFacade<TKey, TValue>(
                config.ZamzaServerHost,
                dateTimeProvider,
                sp.GetRequiredService<ILogger<ZamzaServerFacade<TKey, TValue>>>());

            var messageProcessor = new MessageProcessor<TKey, TValue>(
                sp.GetRequiredService<TMessageProcessor>(),
                dateTimeProvider,
                sp.GetRequiredService<ILogger<MessageProcessor<TKey, TValue>>>());

            var consumptionController = new ConsumptionController<TKey, TValue>(
                config,
                kafkaConsumer,
                zamzaServerFacade,
                messageProcessor,
                dateTimeProvider,
                sp.GetRequiredService<ILogger<ConsumptionController<TKey, TValue>>>());

            kafkaRebalanceListener.SetController(consumptionController);

            return new ZamzaConsumerBackgroundTask<TKey, TValue>(
                consumptionController,
                sp.GetRequiredService<ILogger<ZamzaConsumerBackgroundTask<TKey, TValue>>>());
        });
        
        return services;
    }
}