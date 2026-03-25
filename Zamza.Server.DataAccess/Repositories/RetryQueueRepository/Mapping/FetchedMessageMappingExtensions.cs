using System.Text.Json;
using Zamza.Server.DataAccess.Repositories.RetryQueueRepository.Models;
using Zamza.Server.Models.ConsumerApi.Fetch;

namespace Zamza.Server.DataAccess.Repositories.RetryQueueRepository.Mapping;

internal static class FetchedMessageMappingExtensions
{
    public static FetchedMessage ToModel(this FetchedMessageDto dto)
    {
        return new FetchedMessage(
            dto.Topic,
            dto.Partition,
            dto.Offset,
            ToDictionary(dto.HeadersJson),
            dto.Key,
            dto.Value,
            dto.Timestamp,
            dto.MaxRetriesCount,
            dto.RetriesCount,
            dto.ProcessingDeadlineUtc);
    }

    private static Dictionary<string, byte[]> ToDictionary(string dictionaryJson)
    {
        return JsonSerializer.Deserialize<Dictionary<string, byte[]>>(dictionaryJson) ?? [];
    }
}