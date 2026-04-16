using Confluent.Kafka;

namespace Zamza.Consumer.Internal.Configs.Specific;

internal sealed record MainInfoConfig(
    string ConsumerId,
    string ConsumerGroup,
    ConsumerConfig KafkaConsumerConfig,
    Uri ZamzaServerUri,
    IEnumerable<string> Topics);