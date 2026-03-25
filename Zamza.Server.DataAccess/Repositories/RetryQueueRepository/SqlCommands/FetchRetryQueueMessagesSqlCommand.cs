using Dapper;
using Zamza.Server.DataAccess.Repositories.RetryQueueRepository.Models;

namespace Zamza.Server.DataAccess.Repositories.RetryQueueRepository.SqlCommands;

internal static class FetchRetryQueueMessagesSqlCommand
{
    private const int TimeoutInSeconds = 10;
    
    private const string Sql = 
    $"""
        select
            consumer_group              as {nameof(FetchedMessageDto.ConsumerGroup)},
            retry_queue.topic           as {nameof(FetchedMessageDto.Topic)},
            retry_queue.partition       as {nameof(FetchedMessageDto.Partition)},
            offset_value                as {nameof(FetchedMessageDto.Offset)},
            headers                     as {nameof(FetchedMessageDto.HeadersJson)},
            key                         as {nameof(FetchedMessageDto.Key)},
            value                       as {nameof(FetchedMessageDto.Value)},
            timestamp                   as {nameof(FetchedMessageDto.Timestamp)},
            max_retries_count           as {nameof(FetchedMessageDto.MaxRetriesCount)},
            retries_count               as {nameof(FetchedMessageDto.RetriesCount)},
            processing_deadline_utc     as {nameof(FetchedMessageDto.ProcessingDeadlineUtc)}
        from zamza.retry_queue retry_queue
        join 
            unnest 
                (@Topics, @Partitions, @KafkaOffsets) as fetched_partition(topic, partition, kafka_offset)
            on
                retry_queue.consumer_group = @ConsumerGroup and
                retry_queue.topic = fetched_partition.topic and
                retry_queue.partition = fetched_partition.partition and
                retry_queue.offset_value < fetched_partition.kafka_offset
        where
            retry_queue.next_retry_after < now()
        order by
            retry_queue.offset_value, retry_queue.next_retry_after
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
                KafkaOffsets = kafkaOffsets,
                Limit = limit
            },
            commandTimeout: TimeoutInSeconds,
            cancellationToken: cancellationToken);
    }
}