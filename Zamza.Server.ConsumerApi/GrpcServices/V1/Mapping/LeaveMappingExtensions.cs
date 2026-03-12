using ModelLeaveRequest = Zamza.Server.Application.ConsumerApi.Leave.Models.LeaveRequest;
using GrpcLeaveRequest = Zamza.ConsumerApi.V1.LeaveRequest;

namespace Zamza.Server.ConsumerApi.GrpcServices.V1.Mapping;

internal static class LeaveMappingExtensions
{
    public static ModelLeaveRequest ToModel(this GrpcLeaveRequest grpcLeaveRequest)
    {
        return new ModelLeaveRequest(
            grpcLeaveRequest.ConsumerId,
            grpcLeaveRequest.ConsumerGroup);
    }
}