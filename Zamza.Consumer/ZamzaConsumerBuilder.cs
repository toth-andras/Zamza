using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Zamza.Consumer.Internal;
using Zamza.Consumer.Internal.Configs;
using Zamza.Consumer.Internal.Configs.Specific;
using Zamza.Consumer.Internal.ConsumerConfigValidation;
using Zamza.Consumer.Internal.KafkaConsumerFacade;
using Zamza.Consumer.Internal.MessageProcessing;
using Zamza.Consumer.Internal.Utils.DateTimeProvider;
using Zamza.Consumer.Internal.ZamzaServer;
using Zamza.Consumer.Options;

namespace Zamza.Consumer;

public sealed class ZamzaConsumerBuilder<TKey, TValue, TProcessor>
    where TProcessor : IMessageCustomProcessor<TKey, TValue>
{
    private readonly MainInfoConfig _mainInfoConfig;
    private ZamzaFetchConfig _zamzaFetchConfig;
    private MessageProcessorConfig<TKey, TValue> _messageProcessorConfig;
    private PingConfig _pingConfig;

    public ZamzaConsumerBuilder(
        ConsumerConfig kafkaConsumerConfig,
        Uri zamzaServerUri,
        IEnumerable<string> topics)
    {
        ArgumentNullException.ThrowIfNull(kafkaConsumerConfig);
        ArgumentNullException.ThrowIfNull(zamzaServerUri);
        ArgumentNullException.ThrowIfNull(topics);

        KafkaConsumerConfigValidator.Validate(kafkaConsumerConfig);
        
        _mainInfoConfig = new MainInfoConfig(
            Guid.CreateVersion7().ToString(),
            kafkaConsumerConfig.GroupId,
            kafkaConsumerConfig,
            zamzaServerUri,
            topics);
        
        _zamzaFetchConfig = ZamzaFetchConfig.Default;
        _messageProcessorConfig = MessageProcessorConfig<TKey, TValue>.Default;
        _pingConfig = PingConfig.Default;
    } 

    public IZamzaConsumer Build(IServiceProvider serviceProvider)
    {
        IDateTimeProvider dateTimeProvider = new DateTimeProvider();
        
        var consumerConfig = new ZamzaConsumerConfig<TKey, TValue>(
            _mainInfoConfig,
            _messageProcessorConfig,
            _zamzaFetchConfig,
            _pingConfig);

        var kafkaConsumerFacade = new KafkaConsumerFacade<TKey, TValue>(
            consumerConfig,
            dateTimeProvider);

        var zamzaServerFacade = new ZamzaServerFacade<TKey, TValue>(
            _mainInfoConfig.ZamzaServerUri,
            dateTimeProvider,
            serviceProvider.GetRequiredService<ILogger<ZamzaServerFacade<TKey, TValue>>>());
        
        var messageProcessor = new MessageProcessor<TKey, TValue>(
            serviceProvider.GetRequiredService<TProcessor>(),
            dateTimeProvider,
            serviceProvider.GetRequiredService<ILogger<MessageProcessor<TKey, TValue>>>());
        
        return new ZamzaConsumer<TKey, TValue>(
            consumerConfig,
            kafkaConsumerFacade,
            zamzaServerFacade,
            messageProcessor,
            serviceProvider.GetRequiredService<ILogger<ZamzaConsumer<TKey, TValue>>>(),
            dateTimeProvider);
    }

    public ZamzaConsumerBuilder<TKey, TValue, TProcessor> ConfigureZamzaFetch(ZamzaFetchOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        
        _zamzaFetchConfig = new ZamzaFetchConfig(
            options.KafkaConsumesPerZamzaFetch,
            options.FetchLimit);

        return this;
    }

    public ZamzaConsumerBuilder<TKey, TValue, TProcessor> ConfigureMessageProcessing(
        MessageProcessingOptions<TKey, TValue> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        
        _messageProcessorConfig = new MessageProcessorConfig<TKey, TValue>(
            options.MaxRetriesCount,
            options.MinRetriesGap,
            options.ProcessingPeriod,
            options.RetryGapEvaluator);

        return this;
    }

    public ZamzaConsumerBuilder<TKey, TValue, TProcessor> ConfigureRecovery(
        RecoveryOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        _pingConfig = new PingConfig(
            options.PingInterval,
            options.MaxOfflineTime);
        
        return this;
    }
}