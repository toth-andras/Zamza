using GrpcCurrentPartitionOwner = Zamza.ConsumerApi.V1.CurrentPartitionOwner;
using ModelCurrentPartitionOwner = Zamza.Server.Models.ConsumerApi.PartitionOwnership;

namespace Zamza.Server.ConsumerApi.GrpcServices.V1.Mapping;

internal static class PartitionOwnershipMappingExtensions
{
    public static GrpcCurrentPartitionOwner ToGrpc(this ModelCurrentPartitionOwner partitionOwner)
    {
        return new GrpcCurrentPartitionOwner
        {
            Topic = partitionOwner.Topic,
            Partition = partitionOwner.Partition,
            OwnershipEpoch = partitionOwner.Epoch
        };
    }
}