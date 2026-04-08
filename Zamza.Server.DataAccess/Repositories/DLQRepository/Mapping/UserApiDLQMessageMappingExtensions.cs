using System.Text.Json;
using Zamza.Server.DataAccess.Repositories.DLQRepository.Models;
using Zamza.Server.Models.UserApi;

namespace Zamza.Server.DataAccess.Repositories.DLQRepository.Mapping;

internal static class UserApiDLQMessageMappingExtensions
{
    public static UserApiDLQMessage ToModel(this UserApiDLQMessageDto dto)
    {
        return new UserApiDLQMessage(
            dto.Id,
            dto.ConsumerGroup,
            dto.Topic,
            dto.Partition,
            dto.Offset,
            ConvertHeaders(dto.HeadersJson),
            dto.Key,
            dto.Value,
            dto.Timestamp,
            dto.RetriesCount,
            dto.SavedToDLQAtUTC);
    }

    private static IReadOnlyDictionary<string, byte[]> ConvertHeaders(string headersJson)
    {
        return JsonSerializer.Deserialize<Dictionary<string, byte[]>>(headersJson) ?? [];
    }
}