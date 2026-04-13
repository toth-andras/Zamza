using System.Text.Json;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.AspNetCore.Mvc.TagHelpers.Cache;
using Zamza.ConsumerApi.V1;

namespace Zamza.Consumer.Internal.ZamzaServer.Mapping;

internal static class ZamzaMessageMappingExtensions
{
    public static ZamzaMessage<TKey, TValue> ToModel<TKey, TValue>(this ConsumerApiMessageCore message)
    {
        return new ZamzaMessage<TKey, TValue>(
            message.Topic,
            message.Partition,
            message.Offset,
            message.Headers.ToDictionary(
                header => header.Key,
                header => header.Value.ToByteArray()),
            BytesToType<TKey>(message.Key),
            BytesToType<TValue>(message.Value),
            message.Timestamp.ToDateTime(),
            message.RetriesCount,
            message.MaxRetriesCount,
            message.ProcessingDeadlineUtc?.ToDateTime());
    }

    public static ConsumerApiMessageCore ToGrpc<TKey, TValue>(this ZamzaMessage<TKey, TValue> message)
    {
        return new ConsumerApiMessageCore
        {
            Topic = message.Topic,
            Partition = message.Partition,
            Offset = message.Offset,
            Headers =
            {
                message.Headers?.ToDictionary(
                    header => header.Key,
                    header => ByteString.CopyFrom(header.Value)) ?? []
            },
            Key = TypeToBytes(message.Key),
            Value = TypeToBytes(message.Value),
            Timestamp = message.Timestamp.ToTimestamp(),
            MaxRetriesCount = message.MaxRetriesCount,
            RetriesCount = message.RetriesCount,
            ProcessingDeadlineUtc = message.ProcessingDeadline?.ToTimestamp()
        };
    }

    private static T? BytesToType<T>(ByteString? bytes) 
    {
        if (bytes is null)
        {
            return default;
        }

        return JsonSerializer.Deserialize<T>(bytes.Span) ?? default;
    }

    private static ByteString TypeToBytes<T>(T value)
    {
        return ByteString.CopyFrom(JsonSerializer.SerializeToUtf8Bytes(value));
    }
}