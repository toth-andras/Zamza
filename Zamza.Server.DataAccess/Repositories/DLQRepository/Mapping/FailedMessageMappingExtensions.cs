using System.Text.Json;
using Zamza.Server.DataAccess.Repositories.DLQRepository.Models;
using Zamza.Server.Models.ConsumerApi.Commit;

namespace Zamza.Server.DataAccess.Repositories.DLQRepository.Mapping;

internal static class FailedMessageMappingExtensions
{
    public static FailedMessageDto ToDto(this FailedMessage message)
    {
        return new FailedMessageDto
        {
            Topic = message.Topic,
            Partition = message.Partition,
            Offset = message.Offset,
            HeadersJson = JsonSerializer.Serialize(message.Headers),
            Key = message.Key,
            Value = message.Value,
            Timestamp = message.Timestamp,
            RetriesCount = message.RetriesCount,
            FailedAtUtc = message.FailedAtUtc
        };
    }
}