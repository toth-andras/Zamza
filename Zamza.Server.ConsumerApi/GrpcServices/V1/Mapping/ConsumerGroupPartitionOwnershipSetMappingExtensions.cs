using GrpcOwnershipSet = Zamza.ConsumerApi.V1.ConsumerGroupPartitionOwnershipSet;
using ModelPartitionOwnershipSet = Zamza.Server.Models.ConsumerApi.Common.ConsumerGroupPartitionOwnershipSet;
using GrpcPartitionOwnership = Zamza.ConsumerApi.V1.ConsumerGroupPartitionOwnershipSet.Types.PartitionOwnership;
using ModelPartitionOwnership = Zamza.Server.Models.ConsumerApi.Common.ConsumerGroupPartitionOwnership;

namespace Zamza.Server.ConsumerApi.GrpcServices.V1.Mapping;

internal static class ConsumerGroupPartitionOwnershipSetMappingExtensions
{
    public static GrpcOwnershipSet ToGrpc(this ModelPartitionOwnershipSet partitionOwnershipSet)
    {
        return new GrpcOwnershipSet
        {
            ConsumerGroup = partitionOwnershipSet.ConsumerGroup,
            PartitionOwnerships =
            {
                partitionOwnershipSet.Select(partitionOwnership => partitionOwnership.ToGrpc())
            }
        };
    }

    private static GrpcPartitionOwnership ToGrpc(this ModelPartitionOwnership partitionOwnership)
    {
        return new GrpcPartitionOwnership
        {
            Topic = partitionOwnership.Topic,
            Partition = partitionOwnership.Partition,
            OwnerEpoch = partitionOwnership.OwnerEpoch
        };
    }
}