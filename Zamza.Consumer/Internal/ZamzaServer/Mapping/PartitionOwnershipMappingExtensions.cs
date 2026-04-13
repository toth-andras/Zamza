using PartitionOwnership = Zamza.Consumer.Internal.Models.PartitionOwnership;

namespace Zamza.Consumer.Internal.ZamzaServer.Mapping;

internal static class PartitionOwnershipMappingExtensions
{
    public static PartitionOwnership ToModel(this ConsumerApi.V1.PartitionOwnership ownership)
    {
        return new PartitionOwnership(
            ownership.Topic,
            ownership.Partition,
            ownership.OwnerEpoch);
    }

    public static ConsumerApi.V1.PartitionOwnership ToGrpc(this PartitionOwnership ownership)
    {
        return new ConsumerApi.V1.PartitionOwnership
        {
            Topic = ownership.Topic,
            Partition = ownership.Partition,
            OwnerEpoch = ownership.OwnerEpoch
        };
    }
}