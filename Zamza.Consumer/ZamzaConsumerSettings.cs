using Confluent.Kafka;
using Zamza.Consumer.Models;

namespace Zamza.Consumer;

public sealed record ZamzaConsumerSettings<TKey, TValue>(
    ConsumerConfig KafkaConsumerConfig,
    Uri ZamzaServerUri,
    string BearerToken,
    string ConsumerGroup,
    IReadOnlyCollection<string> Topics,
    int ZamzaCallPerKafkaCalls,
    int MaxRetries,
    TimeSpan MinRetriesGap,
    TimeSpan? ProcessingPeriod,
    Func<ZamzaMessage<TKey, TValue>, TimeSpan> RetryGapEvaluator,
    int FetchLimit);