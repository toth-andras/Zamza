using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.Models;
using Zamza.Server.Models.ConsumerApi.Common;

namespace Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.Mapping;

internal static class PartitionOwnershipMappingExtensions
{
    public static ConsumerGroupPartitionOwnership ToModel(this PartitionOwnershipDto dto)
    {
        return new ConsumerGroupPartitionOwnership(
            dto.Topic,
            dto.Partition,
            dto.Epoch,
            dto.ConsumerId,
            dto.TimestampUtc);
    }

    public static PartitionOwnershipDto ToDto(this ConsumerGroupPartitionOwnership model, string consumerGroup)
    {
        return new PartitionOwnershipDto
        {
            ConsumerGroup = consumerGroup,
            Topic = model.Topic,
            Partition = model.Partition,
            Epoch = model.OwnerEpoch,
            ConsumerId = model.OwnerConsumerId,
            TimestampUtc = model.TimestampUtc
        };
    }
}