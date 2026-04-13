using Zamza.ConsumerApi.V1;

namespace Zamza.Consumer.Internal.ZamzaServer.Mapping;

internal static class LeaveMappingExtensions
{
    public static LeaveRequest ToGrpc(this Models.LeaveRequest request)
    {
        return new LeaveRequest
        {
            ConsumerId = request.ConsumerId,
            ConsumerGroup = request.ConsumerGroup
        };
    }
}