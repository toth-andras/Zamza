using Zamza.ConsumerApi.V1;
using PartitionOwnership = Zamza.Consumer.Internal.Models.PartitionOwnership;

namespace Zamza.Consumer.Internal.ZamzaServer.Mapping;

internal static class ClaimPartitionOwnershipMappingExtensions
{
    public static ClaimPartitionOwnershipRequest ToGrpc(
        this Models.ClaimPartitionOwnershipRequest request)
    {
        return new ClaimPartitionOwnershipRequest
        {
            ConsumerId = request.ConsumerId,
            ConsumerGroup = request.ConsumerGroup,
            PartitionClaims =
            {
                request.ClaimedPartitions.Select(partition => partition.ToGrpc())
            }
        };
    }
    private static ClaimPartitionOwnershipRequest.Types.PartitionClaim ToGrpc(
         this PartitionOwnership claimedPartition)
    {
        return new ClaimPartitionOwnershipRequest.Types.PartitionClaim
        {
            Topic = claimedPartition.Topic,
            Partition = claimedPartition.Partition,
            CurrentOwnerEpoch = claimedPartition.OwnerEpoch
        };
    }
}