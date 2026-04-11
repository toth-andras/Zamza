using Confluent.Kafka;

namespace Zamza.Consumer;

/// <summary>
/// Zamza.Consumer settings.
/// </summary>
/// <param name="KafkaConsumerConfig">
/// The config of Kafka consumer, used internally for consumption from Kafka.
/// </param>
/// <param name="ZamzaServerHost">The host of Zamza.Server.</param>
/// <param name="KafkaCallsPerZamzaCall">
/// When fetching messages, a fetch from Zamza.Server will be done after
/// every KafkaCallsPerZamzaCall fetches from Kafka.
/// </param>
/// <param name="FetchLimit">
/// No more than this many messages will be fetched from Zamza.Server in one request.
/// </param>
/// <param name="ProcessorConfig">Sets up the main behavior of message processing.</param>
/// <param name="PingConfig">Sets up the behavior in case Zamza.Server is not available.</param>
/// <typeparam name="TKey">The type of the message key.</typeparam>
/// <typeparam name="TValue">The type of the message value.</typeparam>
public sealed record ZamzaConsumerSettings<TKey, TValue>(
    ConsumerConfig KafkaConsumerConfig,
    Uri ZamzaServerHost,
    int KafkaCallsPerZamzaCall,
    int FetchLimit,
    ZamzaConsumerSettings<TKey, TValue>.ProcessorSettings<TKey, TValue> ProcessorConfig,
    ZamzaConsumerSettings<TKey, TValue>.ServerAvailabilitySettings PingConfig)
{
    public sealed record ProcessorSettings<TKey, TValue>
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
        public Func<ZamzaMessage<TKey, TValue>, TimeSpan>? RetryGapEvaluator = null;
    }
    
    /// <param name="PingInterval">The interval between ping calls to server.</param>
    /// <param name="MaxOfflineTime">
    /// The maximum time Zamza.Server is allowed not to respond before being marked as unavailable.
    /// </param>
    public sealed record ServerAvailabilitySettings(
        TimeSpan PingInterval,
        TimeSpan MaxOfflineTime);
}