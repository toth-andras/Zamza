using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.Models;

namespace Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.SqlCommands;

internal sealed class ListConsumerOwnershipsSqlCommand
{
    public const int TimeoutInSecond = 30;
    public const string Sql = 
    $"""
        select
            consumer_group  as {nameof(PartitionOwnershipDto.ConsumerGroup)},
            topic           as {nameof(PartitionOwnershipDto.Topic)},
            partition       as {nameof(PartitionOwnershipDto.Partition)},
            epoch           as {nameof(PartitionOwnershipDto.Epoch)},
            consumer_id     as {nameof(PartitionOwnershipDto.ConsumerId)},
            timestamp_utc   as {nameof(PartitionOwnershipDto.TimestampUtc)}
        from zamza.partition_ownership; 
    """;
}