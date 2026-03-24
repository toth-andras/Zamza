using Zamza.ConsumerApi.V1;
using Zamza.Server.Application.ConsumerApi.ClaimPartitionOwnership.Models;
using Zamza.Server.Models.ConsumerApi.ClaimPartitionOwnership;

using GrpcRequest = Zamza.ConsumerApi.V1.ClaimPartitionOwnershipRequest;
using ModelRequest = Zamza.Server.Application.ConsumerApi.ClaimPartitionOwnership.Models.ClaimPartitionOwnershipRequest;
using GrpcClaimedPartition = Zamza.ConsumerApi.V1.ClaimPartitionOwnershipRequest.Types.PartitionClaim;
using ModelClaimedPartition = Zamza.Server.Models.ConsumerApi.ClaimPartitionOwnership.ClaimedPartition;
using GrpcResponse = Zamza.ConsumerApi.V1.ClaimPartitionOwnershipResponse;
using ModelResponse = Zamza.Server.Application.ConsumerApi.ClaimPartitionOwnership.Models.ClaimPartitionOwnershipResponse;

namespace Zamza.Server.ConsumerApi.GrpcServices.V1.Mapping;

internal static class ClaimPartitionOwnershipMappingExtensions
{
    public static ModelRequest ToModel(this GrpcRequest request, DateTimeOffset timestamp)
    {
        var claimsSet = new PartitionOwnershipClaimSet(
            request.ConsumerId,
            request.ConsumerGroup,
            request.PartitionClaims.Select(claim => claim.ToModel()),
            timestamp);

        return new ModelRequest(claimsSet);
    }

    public static GrpcResponse ToGrpc(this ModelResponse modelResponse)
    {
        var consumerGroupPartitionOwnership = modelResponse.ConsumerGroupPartitionOwnerships.ToGrpc();

        return modelResponse.Result switch
        {
            OwnershipClaimResult.Obsolete => AsObsoleteClaimResponse(consumerGroupPartitionOwnership),
            OwnershipClaimResult.Ok => AsOkResponse(consumerGroupPartitionOwnership),

            _ => throw new ArgumentOutOfRangeException(
                paramName: "Claim partition ownership result",
                message: "Not supported result",
                actualValue: modelResponse.Result)
        };
    }

    private static ModelClaimedPartition ToModel(this GrpcClaimedPartition claimedPartition)
    {
        return new ModelClaimedPartition(
            claimedPartition.Topic,
            claimedPartition.Partition,
            claimedPartition.CurrentOwnerEpoch);
    }

    private static GrpcResponse AsOkResponse(ConsumerGroupPartitionOwnershipSet consumerGroupPartitionOwnershipSet)
    {
        return new GrpcResponse
        {
            CurrentOwnershipsForConsumerGroup = consumerGroupPartitionOwnershipSet,
            Ok = new GrpcResponse.Types.ClaimPartitionOwnershipResultOk()
        };
    }
    private static GrpcResponse AsObsoleteClaimResponse(ConsumerGroupPartitionOwnershipSet consumerGroupPartitionOwnershipSet)
    {
        return new GrpcResponse
        {
            CurrentOwnershipsForConsumerGroup = consumerGroupPartitionOwnershipSet,
            ObsoleteClaim = new GrpcResponse.Types.ClaimPartitionOwnershipResultObsoleteClaim()
        };
    }
}