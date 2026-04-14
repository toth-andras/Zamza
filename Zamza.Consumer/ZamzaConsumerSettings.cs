using Confluent.Kafka;
using Zamza.Consumer.Internal.ConsumerConfigValidation;

namespace Zamza.Consumer;

/// <summary>
/// Zamza.Consumer settings.
/// </summary>
/// <typeparam name="TKey">The type of the message key.</typeparam>
/// <typeparam name="TValue">The type of the message value.</typeparam>
public sealed record ZamzaConsumerSettings<TKey, TValue>
{
    /// <summary>
    /// The config of Kafka consumer, used internally for consumption from Kafka.
    /// </summary>
    public ConsumerConfig KafkaConsumerConfig { get; }
    
    /// <summary>
    /// The host of Zamza.Server.
    /// </summary>
    public Uri ZamzaServerHost { get; }
    
    /// <summary>
    /// When fetching messages, a fetch from Zamza.Server will be done after
    /// every KafkaCallsPerZamzaCall fetches from Kafka.
    /// </summary>
    public int KafkaCallsPerZamzaCall { get; }
    
    /// <summary>
    /// No more than this many messages will be fetched from Zamza.Server in one request.
    /// </summary>
    public int FetchLimit { get; }
    
    /// <summary>
    /// Sets up the main behavior of message processing.
    /// </summary>
    public ProcessorSettings ProcessorConfig { get; }
    
    /// <summary>
    /// Sets up the behavior in case Zamza.Server is not available.
    /// </summary>
    public ServerAvailabilitySettings PingConfig { get; }
    
    internal string ConsumerId { get; }
    internal string ConsumerGroup { get; }

    public ZamzaConsumerSettings(
        ConsumerConfig kafkaConsumerConfig,
        Uri zamzaServerHost,
        int kafkaCallsPerZamzaCall,
        int fetchLimit,
        ProcessorSettings processorConfig,
        ServerAvailabilitySettings pingConfig)
    {
        ArgumentNullException.ThrowIfNull(kafkaConsumerConfig);
        ArgumentNullException.ThrowIfNull(zamzaServerHost);
        ArgumentNullException.ThrowIfNull(processorConfig);
        ArgumentNullException.ThrowIfNull(pingConfig);
        
        KafkaConsumerConfigValidator.Validate(kafkaConsumerConfig);
        
        KafkaConsumerConfig = kafkaConsumerConfig;
        ZamzaServerHost = zamzaServerHost;
        KafkaCallsPerZamzaCall = kafkaCallsPerZamzaCall;
        FetchLimit = fetchLimit;
        ProcessorConfig = processorConfig;
        PingConfig = pingConfig;
        
        ConsumerId = Guid.CreateVersion7().ToString();
        ConsumerGroup = kafkaConsumerConfig.GroupId;
    }

    public sealed record ProcessorSettings
    {
        /// <summary>
        /// No more than this many reprocessing attempts will be performed.
        /// </summary>
        public int MaxRetriesCount { get; init; } = int.MaxValue;
        
        /// <summary>
        /// The minimal delay between two reprocessing attempts.
        /// </summary>
        public TimeSpan MinRetriesGap { get; init; } = TimeSpan.FromSeconds(5);
        
        /// <summary>
        /// Represents the period of time after the initial processing, in which
        /// the message must be successfully reprocessed, otherwise it will be
        /// considered to be poisoned.
        /// </summary>
        public TimeSpan? ProcessingPeriod { get; init; } = null;

        /// <summary>
        /// Evaluates the delay before the next reprocessing attempt.
        /// </summary>
        public Func<ZamzaMessage<TKey, TValue>, TimeSpan>? RetryGapEvaluator { get; init; } = null;
    }
    
    public sealed record ServerAvailabilitySettings
    {
        /// <summary>
        /// The interval between ping calls to server.
        /// </summary>
        public TimeSpan PingInterval { get; init; } = TimeSpan.FromSeconds(10);
        
        /// <summary>
        /// The maximum time Zamza.Server is allowed not to respond before being marked as unavailable.
        /// </summary>
        public TimeSpan MaxOfflineTime { get; init; } = TimeSpan.FromMinutes(5);
    }
}