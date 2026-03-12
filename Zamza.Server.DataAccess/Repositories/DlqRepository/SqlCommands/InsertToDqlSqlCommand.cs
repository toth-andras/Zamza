using System.Text.Json;
using Dapper;
using Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;

namespace Zamza.Server.DataAccess.Repositories.DlqRepository.SqlCommands;

internal static class InsertToDqlSqlCommand
{
    private const int TimeoutInSeconds = 10;
    
    private const string Sql = 
    """
        insert into zamza.dlq
        (
            consumer_group,
            topic,
            partition,
            offset_value,
            headers,
            key,
            value,
            timestamp,
            retries_count,
            became_poisoned_at_utc,
            version
        )
        select
            @ConsumerGroup,
            u.topic,
            u.partition,
            u.offset_value,
            u.headers,
            u.key,
            u.value,
            u.timestamp,
            u.retries_count,
            @BecamePoisonedAtUtc,
            1
        from unnest
        (
            @TopicValues,
            @PartitionValues,
            @OffsetValues,
            @HeadersValues::jsonb[],
            @KeyValues,
            @Values,
            @TimestampValues,
            @RetriesCountValues
        ) as u (topic, partition, offset_value, headers, key, value, timestamp, retries_count)
        on conflict (consumer_group, topic, partition, offset_value) 
        do update set
            retries_count = excluded.retries_count,
            became_poisoned_at_utc = excluded.became_poisoned_at_utc;
    """;

    public static CommandDefinition BuildCommandDefinition(
        IDbTransactionFrame transaction,
        string consumerGroup,
        string[] topicValues,
        int[] partitionValues,
        long[] offsetValues,
        Dictionary<string, byte[]>[] headersValues,
        byte[]?[] keyValues,
        byte[]?[] values,
        DateTimeOffset[] timestampValues,
        int[] retriesCountValues,
        DateTimeOffset becamePoisonedAtUtc,
        CancellationToken cancellationToken)
    {
        return new CommandDefinition(
            commandText: Sql,
            parameters: new
            {
                ConsumerGroup = consumerGroup,
                TopicValues = topicValues,
                PartitionValues = partitionValues,
                OffsetValues = offsetValues,
                HeadersValues = headersValues
                    .Select(h => JsonSerializer.Serialize(h))
                    .ToArray(),
                KeyValues = keyValues,
                Values = values,
                TimestampValues = timestampValues,
                RetriesCountValues = retriesCountValues,
                BecamePoisonedAtUtc = becamePoisonedAtUtc
            },
            transaction: transaction.Transaction,
            commandTimeout: TimeoutInSeconds,
            cancellationToken: cancellationToken);
    }
}