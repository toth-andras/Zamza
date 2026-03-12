using System.Text.Json;
using Dapper;
using Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;

namespace Zamza.Server.DataAccess.Repositories.RetryQueueRepository.SqlCommands;

internal static class InsertToRetryQueueSqlCommand
{
    private const int TimeoutInSeconds = 10;
    private const string Sql = 
    """
        insert into zamza.retry_queue
        (
            consumer_group,
            topic,
            partition,
            offset_value,
            headers,
            key,
            value,
            timestamp,
            max_retries,
            min_retries_gap_ms,
            processing_period_ms,
            retries_count,
            next_retry_after,
            processing_deadline,
            last_retry_at_utc,
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
            u.max_retries,
            u.min_retries_gap_ms,
            u.processing_period_ms,
            u.retries_count,
            now() + (u.next_retry_after || ' milliseconds')::interval,
            now() + (u.processing_period_ms || ' milliseconds')::interval,
            u.last_retry_at_utc,
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
             @MaxRetriesValues,
             @MinRetriesGapValues,
             @ProcessingPeriodValues,
             @RetriesCountValues,
             @NextRetryAfterValues,
             @LastRetryAtUtcValues
        ) as u (topic, partition, offset_value, headers, key, value, timestamp, max_retries, min_retries_gap_ms, processing_period_ms, retries_count, next_retry_after, last_retry_at_utc)
        on conflict (consumer_group, topic, partition, offset_value)
        do update set
            max_retries = excluded.max_retries,
            min_retries_gap_ms = excluded.min_retries_gap_ms,
            processing_period_ms = excluded.processing_period_ms,
            retries_count = excluded.retries_count,
            next_retry_after = excluded.next_retry_after,
            processing_deadline = excluded.processing_deadline,
            last_retry_at_utc = excluded.last_retry_at_utc;
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
        DateTimeOffset[]  timestampValues,
        int[] maxRetriesValues,
        long[] minRetriesGapMsValues,
        long?[] processingPeriodMsValues,
        int[] retriesCountValues,
        long[] nextRetryAfterValues,
        DateTimeOffset[] lastRetryAtUtcValues,
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
                MaxRetriesValues = maxRetriesValues,
                MinRetriesGapValues = minRetriesGapMsValues,
                ProcessingPeriodValues = processingPeriodMsValues,
                RetriesCountValues = retriesCountValues,
                NextRetryAfterValues = nextRetryAfterValues,
                LastRetryAtUtcValues = lastRetryAtUtcValues
            },
            transaction: transaction.Transaction,
            commandTimeout: TimeoutInSeconds,
            cancellationToken: cancellationToken);
    }
}