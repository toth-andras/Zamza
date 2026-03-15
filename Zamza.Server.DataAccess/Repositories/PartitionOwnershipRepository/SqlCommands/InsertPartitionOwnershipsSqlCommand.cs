using Dapper;
using Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;

namespace Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.SqlCommands;

internal static class InsertPartitionOwnershipsSqlCommand
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
            @Timestamp
        from unnest
        (
            @Topics,
            @Partitions,
            @Epochs,
            @ConsumerIds
        ) as u(topic, partition, epoch, consumer_id)
        on conflict (consumer_group, topic, partition)
        do update set
            epoch = excluded.epoch,
            consumer_id =  excluded.consumer_id,
            timestamp_utc = excluded.timestamp_utc;
    """;

    public static CommandDefinition BuildCommandDefinition(
        IDbTransactionFrame transaction,
        string consumerGroup,
        string[] topicValues,
        int[] partitionValues,
        long[] epochValues,
        string[] consumerIdValues,
        DateTimeOffset timestamp,
        CancellationToken cancellationToken)
    {
        return new CommandDefinition(
            commandText: Sql,
            parameters: new
            {
                ConsumerGroup = consumerGroup,
                Topics = topicValues,
                Partitions = partitionValues,
                Epochs = epochValues,
                ConsumerIds = consumerIdValues,
                Timestamp = timestamp
            },
            transaction: transaction.Transaction,
            commandTimeout: TimeoutInSeconds,
            cancellationToken: cancellationToken);
    }
}