using System.Text.Json;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Zamza.Consumer.Models;
using Zamza.ConsumerApi.V1;
using Timestamp = Confluent.Kafka.Timestamp;

namespace Zamza.Consumer.ServerFacade;

internal static class ZamzaMessageFactoryForZamzaServer<TKey, TValue>
{
    public static ZamzaMessage<TKey, TValue> CreateModelMessage(ZamzaMessage source)
    {
        return new ZamzaMessage<TKey, TValue>(
            source.Topic,
            source.Partition,
            source.Offset,
            source.Headers.ToDictionary(
                entry => entry.Key,
                entry => entry.Value.ToByteArray()),
            JsonSerializer.Deserialize<TKey>(source.Key.ToByteArray()),
            JsonSerializer.Deserialize<TValue>(source.Value.ToByteArray()),
            new Timestamp(),
            source.RetriesCount,
            source.MaxRetries,
            TimeSpan.FromMilliseconds(source.MinRetriesGapMs),
            source.ProcessingPeriodMs is null 
                ? null
                : TimeSpan.FromMilliseconds(source.ProcessingPeriodMs.Value));
    }

    public static ZamzaMessage CreateGrpcMessage(
        ZamzaMessage<TKey, TValue> message,
        string consumerGroup)
    {
        return new ZamzaMessage
        {
            ConsumerGroup = consumerGroup,
            Topic = message.Topic,
            Partition = message.Partition,
            Offset = message.Offset,
            Headers =
            {
               message.Headers.ToDictionary(
                   header => header.Key,
                   header => ByteString.CopyFrom(header.Value)) 
            },
            Key = ByteString.CopyFrom(JsonSerializer.SerializeToUtf8Bytes(message.Key)),
            Value = ByteString.CopyFrom(JsonSerializer.SerializeToUtf8Bytes(message.Value)),
            Timestamp = message.Timestamp.UtcDateTime.ToTimestamp(),
            RetriesCount = message.RetriesCount,
            MaxRetries = message.MaxRetries,
            MinRetriesGapMs = (long) message.MinRetriesGap.TotalMilliseconds,
            ProcessingPeriodMs = message.ProcessingPeriod is null ? null : (long)message.ProcessingPeriod.Value.TotalMilliseconds
        };
    }
}