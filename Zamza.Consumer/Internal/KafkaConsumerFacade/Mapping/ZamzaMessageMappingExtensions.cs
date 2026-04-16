using Confluent.Kafka;

namespace Zamza.Consumer.Internal.KafkaConsumerFacade.Mapping;

internal static class ZamzaMessageMappingExtensions
{
    public static ZamzaMessage<TKey, TValue> ToZamzaMessage<TKey, TValue>(
        this ConsumeResult<TKey, TValue> consumeResult,
        int maxRetriesCount,
        DateTime? processingDeadline)
    {
        const int initialRetriesCount = 0;
        
        return new ZamzaMessage<TKey, TValue>(
            consumeResult.Topic,
            consumeResult.Partition.Value,
            consumeResult.Offset.Value,
            consumeResult.Message.Headers.ToDictionary(
                header => header.Key,
                header => header.GetValueBytes()),
            consumeResult.Message.Key,
            consumeResult.Message.Value,
            consumeResult.Message.Timestamp.UtcDateTime,
            initialRetriesCount,
            maxRetriesCount,
            processingDeadline,
            isFromKafka: true);
    }
}