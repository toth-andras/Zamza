using Zamza.Server.Models.UserApi;
using Zamza.Server.UserApi.Controllers.V1.DLQ.Models;

namespace Zamza.Server.UserApi.Controllers.V1.DLQ.Mapping;

internal static class DLQMessageMappingExtensions
{
    public static DLQMessageDto ToRest(this UserApiDLQMessage message)
    {
        return new DLQMessageDto
        {
            ConsumerGroup = message.ConsumerGroup,
            Topic = message.Topic,
            Partition = message.Partition,
            Offset = message.Offset,
            Headers = message.Headers.ToDictionary(),
            Key = message.Key,
            Value = message.Value,
            Timestamp = message.Timestamp,
            RetriesCount = message.RetriesCount,
            SavedToDLQAtUTC = message.SavedToDLQAtUTC
        };
    } 
}