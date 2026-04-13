using Google.Protobuf.WellKnownTypes;
using Zamza.Consumer.Internal.ZamzaServer.Models;
using Zamza.ConsumerApi.V1;

namespace Zamza.Consumer.Internal.ZamzaServer.Mapping;

internal static class CommitMappingExtensions
{
    public static CommitRequest ToGrpc<TKey, TValue>(this CommitRequest<TKey, TValue> request)
    {
        return new CommitRequest
        {
            ConsumerId = request.ConsumerId,
            ConsumerGroup = request.ConsumerGroup,
            OwnershipsForProcessedPartitions =
            {
                request.PartitionsToCommit.Select(partition => partition.ToGrpc())
            },
            ProcessedMessages =
            {
                request.ProcessedMessages.Select(message => new CommitRequest.Types.ProcessedMessage
                {
                    Topic = message.Topic,
                    Partition = message.Partition,
                    Offset = message.Offset
                })
            },
            RetryableMessages =
            {
                request.MessagesWithRetryableFailure.Select(message => new CommitRequest.Types.RetryableMessage
                {
                    Message = message.Message.ToGrpc(),
                    NextRetryAfterMs = (long)message.NextRetryAfter.TotalMilliseconds
                })
            },
            FailedMessages =
            {
                request.MessagesWithCompleteFailure.Select(message => new CommitRequest.Types.FailedMessage
                {
                    Message = message.Message.ToGrpc(),
                    FailedAtUtc = message.FailedAtUtc.ToTimestamp()
                })
            }
        };
    }

    public static CommitResult ToModel(this CommitResponse response)
    {
        return new CommitResult(
            response.CurrentOwnershipsForConsumerGroup.PartitionOwnerships
                .Select(ownership => ownership.ToModel())
                .ToList(),
            response.PartitionsWithIrrelevantOwnership
                .Select(partition => partition.ToModel())
                .ToList());
    }

    private static CommitResult.PartitionWithIrrelevantOwnership ToModel(
        this CommitResponse.Types.PartitionWithIrrelevantOwnership ownership)
    {
        return new CommitResult.PartitionWithIrrelevantOwnership(
            ownership.Topic,
            ownership.Partition);
    }
}