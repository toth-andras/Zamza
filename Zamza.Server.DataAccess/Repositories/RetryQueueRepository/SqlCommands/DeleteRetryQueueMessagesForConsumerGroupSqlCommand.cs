using System.Data.Common;
using Dapper;
using Zamza.Server.DataAccess.Repositories.CommonModels;

namespace Zamza.Server.DataAccess.Repositories.RetryQueueRepository.SqlCommands;

internal static class DeleteRetryQueueMessagesForConsumerGroupSqlCommand
{
    private const int TimeoutInSeconds = 10;
    
    private static string Sql => 
    $"""
        delete from zamza.retry_queue stored_rows
        using unnest
        (
            @{nameof(Parameters.Topics)},
            @{nameof(Parameters.Partitions)},
            @{nameof(Parameters.Offsets)}
        ) as rows_to_delete(topic, partition, offset_value)
        where
            stored_rows.consumer_group = @{nameof(Parameters.ConsumerGroup)} and
            stored_rows.topic = rows_to_delete.topic and
            stored_rows.partition = rows_to_delete.partition and
            stored_rows.offset_value = rows_to_delete.offset_value;
    """;

    public static CommandDefinition BuildCommandDefinition(
        DbTransaction  transaction,
        string consumerGroup,
        IReadOnlyCollection<MessageToDelete> messages,
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
        IReadOnlyCollection<MessageToDelete> messages)
    {
        var topics = new string[messages.Count];
        var partitions = new int[messages.Count];
        var offsets = new long[messages.Count];

        var index = 0;
        foreach (var message in messages)
        {
            topics[index] = message.Topic;
            partitions[index] = message.Partition;
            offsets[index] = message.Offset;
            index++;
        }
        
        return new Parameters(
            consumerGroup,
            topics,
            partitions,
            offsets);
    }
    
    private sealed record Parameters(
        string ConsumerGroup,
        string[] Topics,
        int[] Partitions,
        long[] Offsets);
}