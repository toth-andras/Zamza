using Zamza.Server.DataAccess.Repositories.ConsumerHeartbeatRepository.Models;
using Zamza.Server.Models.ConsumerApi.Monitoring;

namespace Zamza.Server.DataAccess.Repositories.ConsumerHeartbeatRepository.Mapping;

internal static class ConsumerHearbeatMappingExtensions
{
    public static ConsumerHeartbeatDto ToDto(this ConsumerHeartbeat heartbeat)
    {
        return new ConsumerHeartbeatDto
        {
            ConsumerId = heartbeat.ConsumerId,
            ConsumerGroup = heartbeat.ConsumerGroup,
            TimestampUtc = heartbeat.TimestampUtc
        };
    }
}