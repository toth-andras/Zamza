using GrpcPingRequest = Zamza.ConsumerApi.V1.PingRequest;
using ModelPingRequest = Zamza.Server.Application.ConsumerApi.Ping.Models.PingRequest;

using GrpcPingResponse = Zamza.ConsumerApi.V1.PingResponse;
using ModelPingResponse = Zamza.Server.Application.ConsumerApi.Ping.Models.PingResponse;

namespace Zamza.Server.ConsumerApi.GrpcServices.V1.Mapping;

internal static class PingMappingExtensions
{
    public static ModelPingRequest ToModel(this GrpcPingRequest pingRequest)
    {
        return new ModelPingRequest(
            pingRequest.ConsumerId,
            pingRequest.ConsumerGroup);
    }

    public static GrpcPingResponse ToGrpc(this ModelPingResponse pingResponse)
    {
        return new GrpcPingResponse
        {
            CurrentPartitionOwners =
            {
                pingResponse.CurrentPartitionOwners.Select(partitionOwnership => partitionOwnership.ToGrpc())
            }
        };
    }
}