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
}