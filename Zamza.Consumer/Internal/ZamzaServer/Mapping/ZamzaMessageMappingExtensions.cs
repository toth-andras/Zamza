using System.Text.Json;
using Google.Protobuf;
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

    private static T? BytesToType<T>(ByteString? bytes) 
    {
        if (bytes is null)
        {
            return default;
        }

        return JsonSerializer.Deserialize<T>(bytes.Span) ?? default;
    }
}