using System.Text.Json;
using Zamza.Server.DataAccess.Repositories.RetryQueueRepository.Models;
using Zamza.Server.Models.ConsumerApi.Commit;

namespace Zamza.Server.DataAccess.Repositories.RetryQueueRepository.Mapping;

internal static class RetryableMessageMappingExtensions
{
    public static RetryableMessageDto ToDto(this RetryableMessage message)
    {
        return new RetryableMessageDto
        {
            Topic = message.Topic,
            Partition = message.Partition,
            Offset = message.Offset,
            HeadersJson = JsonSerializer.Serialize(message.Headers),
            Key = message.Key,
            Value = message.Value,
            Timestamp = message.Timestamp,
            MaxRetriesCount = message.MaxRetriesCount,
            RetriesCount = message.RetriesCount,
            ProcessingDeadlineUtc = message.ProcessingDeadlineUtc,
            NextRetryAfterMs = message.NextRetryAfterMs
        };
    }
}