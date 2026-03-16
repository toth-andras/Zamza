using Confluent.Kafka;
using Zamza.Consumer.Models;
using Zamza.Consumer.Models.ConsumerMetadata;

namespace Zamza.Consumer.Factories;

internal static class ZamzaMessageFactoryForKafka
{
    public static ZamzaMessage<TKey, TValue> Create<TKey, TValue>(
        ConsumeResult<TKey, TValue> consumeResult,
        ConsumerMetadata<TKey, TValue>  metadata)
    {
        const int initialRetriesCount = 0;
        return new ZamzaMessage<TKey, TValue>(
            consumeResult.Topic,
            consumeResult.Partition.Value,
            consumeResult.Offset.Value,
            consumeResult.Message.Headers.BackingList.ToDictionary(
                header => header.Key,
                header => header.GetValueBytes()),
            consumeResult.Message.Key,
            consumeResult.Message.Value,
            consumeResult.Message.Timestamp,
            initialRetriesCount,
            metadata.MaxRetries,
            metadata.MinRetriesGap,
            metadata.ProcessingPeriod);
    }
}