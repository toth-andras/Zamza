using Zamza.ConsumerApi.V1;

namespace Zamza.Consumer.Internal.ZamzaServer.Mapping;

internal static class FetchMappingExtensions
{
    public static FetchRequest ToGrpc(this Models.FetchRequest request)
    {
        return new FetchRequest
        {
            ConsumerId = request.ConsumerId,
            ConsumerGroup = request.ConsumerGroup,
            Limit = request.Limit,
            Partitions =
            {
                request.FetchedPartitions.Select(partition => partition.ToGrpc())
            }
        };
    }
    
    private static FetchRequest.Types.FetchedPartition ToGrpc(
        this Models.FetchRequest.FetchedPartition fetchedPartition)
    {
        return new FetchRequest.Types.FetchedPartition
        {
            Topic = fetchedPartition.Topic,
            Partition = fetchedPartition.Partition,
            KafkaOffset = fetchedPartition.KafkaOffset,
            OwnershipEpoch = fetchedPartition.OwnershipEpoch
        };
    }
}