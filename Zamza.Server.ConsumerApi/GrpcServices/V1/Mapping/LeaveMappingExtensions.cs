using GrpcRequest = Zamza.ConsumerApi.V1.LeaveRequest;
using ModelRequest = Zamza.Server.Application.ConsumerApi.Leave.Models.LeaveRequest;

namespace Zamza.Server.ConsumerApi.GrpcServices.V1.Mapping;

internal static class LeaveMappingExtensions
{
    public static ModelRequest ToModel(this GrpcRequest request, DateTimeOffset utcNow)
    {
        return new ModelRequest(
            request.ConsumerId,
            request.ConsumerGroup,
            utcNow);
    }
}