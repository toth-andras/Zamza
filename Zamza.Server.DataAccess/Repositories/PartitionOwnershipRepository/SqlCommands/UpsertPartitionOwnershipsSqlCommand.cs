using System.Data.Common;
using Dapper;

namespace Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.SqlCommands;

internal static class UpsertPartitionOwnershipsSqlCommand
{
    private const int TimeoutInSeconds = 10;
    
    private const string Sql = 
    """
        insert into zamza.partition_ownership
        (
            consumer_group,
            topic,
            partition,
            epoch,
            consumer_id,
            timestamp_utc
        )
        select
            @ConsumerGroup,
            u.topic,
            u.partition,
            u.epoch,
            u.consumer_id,
            u.timestamp_utc
        from unnest
        (
            @Topics,
            @Partitions,
            @Epochs,
            @ConsumerIds,
            @Timestamps
        ) as u(topic, partition, epoch, consumer_id, timestamp_utc)
        on conflict (consumer_group, topic, partition)
        do update set
            epoch = excluded.epoch,
            consumer_id = excluded.consumer_id,
            timestamp_utc = excluded.timestamp_utc;
    """;

    public static CommandDefinition BuildCommandDefinition(
        string consumerGroup,
        string[] topics,
        int[] partitions,
        long[] epochs,
        string?[] consumerIds,
        DateTimeOffset[] timestamps,
        DbTransaction transaction,
        CancellationToken cancellationToken)
    {
        return new CommandDefinition(
            commandText: Sql,
            parameters: new
            {
                ConsumerGroup = consumerGroup,
                Topics = topics,
                Partitions = partitions,
                Epochs = epochs,
                ConsumerIds = consumerIds,
                Timestamps = timestamps
            },
            transaction: transaction,
            commandTimeout:  TimeoutInSeconds,
            cancellationToken: cancellationToken);
    }
}