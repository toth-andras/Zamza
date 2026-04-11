using System.Data.Common;
using Dapper;
using Zamza.Server.DataAccess.Repositories.DLQRepository.Models;

namespace Zamza.Server.DataAccess.Repositories.DLQRepository.SqlCommands;

internal static class UpsertDLQMessagesSqlCommand
{
    private const int TimeoutInSeconds = 10;
    private const int RecordVersion = 1;
    
    private static string Sql => 
    $"""
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
            failed_at_utc,
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
            u.retries_count,
            u.failed_at_utc,
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
             @{nameof(Parameters.RetriesCounts)},
             @{nameof(Parameters.BecamePoisonedAtUtcs)}
        ) as u(topic, partition, offset_value, headers_json, key, value, timestamp, retries_count, failed_at_utc)
        on conflict (consumer_group, topic, partition, offset_value)
        do update set
            retries_count = excluded.retries_count,
            failed_at_utc = excluded.timestamp;
    """;

    public static CommandDefinition BuildCommandDefinition(
        DbTransaction transaction,
        string consumerGroup,
        IReadOnlyCollection<FailedMessageDto> messages,
        CancellationToken cancellationToken)
    {
        var parameters = FormParameters(consumerGroup, messages);
        
        return new CommandDefinition(
            commandText: Sql,
            parameters: parameters,
            transaction: transaction,
            commandTimeout: TimeoutInSeconds,
            cancellationToken: cancellationToken);
    }

    private static Parameters FormParameters(
        string consumerGroup,
        IReadOnlyCollection<FailedMessageDto> messages)
    {
        var topics = new string[messages.Count];
        var partitions = new int[messages.Count];
        var offsets = new long[messages.Count];
        var headersJsons = new string[messages.Count];
        var keys = new byte[]?[messages.Count];
        var values = new byte[]?[messages.Count];
        var timestamps = new DateTimeOffset[messages.Count];
        var retriesCounts = new int[messages.Count];
        var becamePoisonedAtUtcs = new DateTimeOffset[messages.Count];

        var index = 0;
        foreach (var message in messages)
        {
            topics[index] = message.Topic;
            partitions[index] = message.Partition;
            offsets[index] = message.Offset;
            headersJsons[index] = message.HeadersJson;
            keys[index] = message.Key;
            values[index] = message.Value;
            timestamps[index] = message.Timestamp;
            retriesCounts[index] = message.RetriesCount;
            becamePoisonedAtUtcs[index] = message.Timestamp;
            index++;
        }

        return new Parameters(
            consumerGroup,
            topics,
            partitions,
            offsets,
            headersJsons,
            keys,
            values,
            timestamps,
            retriesCounts,
            becamePoisonedAtUtcs);
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
        int[] RetriesCounts,
        DateTimeOffset[] BecamePoisonedAtUtcs);
}