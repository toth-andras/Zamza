using System.Data.Common;
using Dapper;
using Zamza.Server.DataAccess.Repositories.RetryQueueRepository.Models;

namespace Zamza.Server.DataAccess.Repositories.RetryQueueRepository.SqlCommands;

internal static class UpsertRetryQueueMessagesSqlCommand
{
    private const int TimeoutInSeconds = 10;
    private const int RecordVersion = 1;
    
    private static string Sql => 
    $"""
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
            max_retries_count,
            retries_count,
            processing_deadline_utc,
            next_retry_after,
            version                              
        )
        select
            @{nameof(Parameters.ConsumerGroup)},
            u.topic,
            u.partition,
            u.offset_value,
            u.headers_json::jsonb,
            u.key,
            u.value,
            u.timestamp,
            u.max_retries_count,
            u.retries_count,
            u.processing_deadline_utc,
            now() + (u.next_retry_after_ms || ' milliseconds')::interval,
            {RecordVersion}
        from unnest
        (
            @{nameof(Parameters.Topics)},
            @{nameof(Parameters.Partitions)},
            @{nameof(Parameters.Offsets)},
            @{nameof(Parameters.HeadersJsons)},
            @{nameof(Parameters.Keys)},
            @{nameof(Parameters.Values)},
            @{nameof(Parameters.Timestamps)},
            @{nameof(Parameters.MaxRetriesCounts)},
            @{nameof(Parameters.RetriesCounts)},
            @{nameof(Parameters.ProcessingDeadlinesUtc)},
            @{nameof(Parameters.NextRetriesAfterMs)},
        ) as u (topic, partition, offset_value, headers_json, key, value, timestamp, max_retries_count, retries_count, processing_deadline_utc, next_retry_after_ms)
        on conflict (consumer_group, topic, partition, offset_value)
            do update set
                max_retries_count = excluded.max_retries_count,
                retries_count = excluded.retries_count,
                processing_deadline_utc = excluded.processing_deadline_utc,
                next_retry_after = excluded.next_retry_after;
    """;

    public static CommandDefinition BuildCommandDefinition(
        DbTransaction transaction,
        string consumerGroup,
        IReadOnlyCollection<RetryableMessageDto> messages,
        CancellationToken cancellationToken)
    {
        var parameters = ToParameters(consumerGroup, messages);
        
        return new CommandDefinition(
            commandText: Sql,
            parameters: parameters,
            transaction: transaction,
            commandTimeout: TimeoutInSeconds,
            cancellationToken: cancellationToken);
    }

    private static Parameters ToParameters(
        string consumerGroup,
        IReadOnlyCollection<RetryableMessageDto> messages)
    {
        var topicValues = new string[messages.Count];
        var partitionValues = new int[messages.Count];
        var offsetValues = new long[messages.Count];
        var headersJsons = new string[messages.Count];
        var keyValues = new byte[]?[messages.Count];
        var valueValues = new byte[]?[messages.Count];
        var timestampValues = new DateTimeOffset[messages.Count];
        var maxRetriesCountValues = new int[messages.Count];
        var retriesCountValues = new int[messages.Count];
        var processingDeadlineValuesUtc = new DateTimeOffset?[messages.Count];
        var nextRetryAfterMsValues = new long[messages.Count];

        var index = 0;
        foreach (var message in messages)
        {
            topicValues[index] = message.Topic;
            partitionValues[index] = message.Partition;
            offsetValues[index] = message.Offset;
            headersJsons[index] = message.HeadersJson;
            keyValues[index] = message.Key;
            valueValues[index] = message.Value;
            timestampValues[index] = message.Timestamp;
            maxRetriesCountValues[index] = message.MaxRetriesCount;
            retriesCountValues[index] = message.RetriesCount;
            processingDeadlineValuesUtc[index] = message.ProcessingDeadlineUtc;
            nextRetryAfterMsValues[index] = message.NextRetryAfterMs;
            index++;
        }

        return new Parameters(
            consumerGroup,
            topicValues,
            partitionValues,
            offsetValues,
            headersJsons,
            keyValues,
            valueValues,
            timestampValues,
            maxRetriesCountValues,
            retriesCountValues,
            processingDeadlineValuesUtc,
            nextRetryAfterMsValues);
    }

    private sealed record Parameters(
        string ConsumerGroup,
        string[] Topics,
        int[] Partitions,
        long[] Offsets,
        string[] HeadersJsons,
        byte[]?[] Keys,
        byte[]?[] Values,
        DateTimeOffset[] Timestamps,
        int[] MaxRetriesCounts,
        int[] RetriesCounts,
        DateTimeOffset?[] ProcessingDeadlinesUtc,
        long[] NextRetriesAfterMs);
}