using Dapper;
using Zamza.Server.Models.ConsumerApi;

namespace Zamza.Server.DataAccess.Repositories.RetryQueueRepository.SqlCommands;

internal static class GetMessagesForFetchSqlCommand
{
    private const int CommandTimeoutSeconds = 10;
    
    private const string Sql = 
    $"""
        select
            consumer_group          as {nameof(ConsumerApiMessage.ConsumerGroup)},
            retry_queue.topic       as {nameof(ConsumerApiMessage.Topic)},
            retry_queue.partition   as {nameof(ConsumerApiMessage.Partition)},
            offset_value            as {nameof(ConsumerApiMessage.Offset)},
            headers                 as {nameof(ConsumerApiMessage.Headers)},
            key                     as {nameof(ConsumerApiMessage.Key)},
            value                   as {nameof(ConsumerApiMessage.Value)},
            timestamp               as {nameof(ConsumerApiMessage.Timestamp)},
            max_retries             as {nameof(ConsumerApiMessage.MaxRetries)},
            min_retries_gap_ms      as {nameof(ConsumerApiMessage.MinRetriesGapMs)},
            processing_period_ms    as {nameof(ConsumerApiMessage.ProcessingPeriodMs)},
            retries_count           as {nameof(ConsumerApiMessage.RetriesCount)}
        from zamza.retry_queue retry_queue
        join 
            unnest(@Topics, @Partitions, @KafkaOffsets) 
                as topic_partition(topic, partition, kafka_offset) 
            on 
                retry_queue.topic = topic_partition.topic
                and retry_queue.partition = topic_partition.partition
        where
            consumer_group = @ConsumerGroup
            and retries_count < max_retries
            and next_retry_after < now()
            and (processing_deadline is null or processing_deadline > now())
            and retry_queue.offset_value < topic_partition.kafka_offset 
        order by
            offset_value, next_retry_after
        limit @Limit;
    """;

    public static CommandDefinition BuildCommandDefinition(
        string consumerGroup,
        string[] topics,
        int[] partitions,
        long[] kafkaOffsets,
        int limit,
        CancellationToken cancellationToken)
    {
        return new CommandDefinition(
            commandText: Sql,
            parameters: new
            {
                ConsumerGroup = consumerGroup,
                Topics = topics,
                Partitions = partitions,
                KafkaOffset = kafkaOffsets,
                Limit = limit
            },
            commandTimeout: CommandTimeoutSeconds,
            cancellationToken: cancellationToken);
    }
}