using Zamza.ConsumerApi.V1;
using Zamza.Server.Application.ConsumerApi.Commit.Models;
using GrpcRequest = Zamza.ConsumerApi.V1.CommitRequest;
using ModelRequest = Zamza.Server.Application.ConsumerApi.Commit.Models.CommitRequest;
using GrpcProcessedMessage = Zamza.ConsumerApi.V1.CommitRequest.Types.ProcessedMessage;
using ModelProcessedMessage = Zamza.Server.Models.ConsumerApi.Commit.ProcessedMessage;
using GrpcRetryableMessage = Zamza.ConsumerApi.V1.CommitRequest.Types.RetryableMessage;
using ModelRetryableMessage = Zamza.Server.Models.ConsumerApi.Commit.RetryableMessage;
using GrpcFailedMessage = Zamza.ConsumerApi.V1.CommitRequest.Types.FailedMessage;
using ModelFailedMessage = Zamza.Server.Models.ConsumerApi.Commit.FailedMessage;
using GrpcCommitResponse = Zamza.ConsumerApi.V1.CommitResponse;
using ModelCommitResponse = Zamza.Server.Application.ConsumerApi.Commit.Models.CommitResponse;
using GrpcPartitionWithIrrelevantOwnership = Zamza.ConsumerApi.V1.CommitResponse.Types.PartitionWithIrrelevantOwnership;
using ModelPartitionWithIrrelevantOwnership = Zamza.Server.Application.ConsumerApi.Commit.Models.PartitionWithIrrelevantOwnership;

namespace Zamza.Server.ConsumerApi.GrpcServices.V1.Mapping;

internal static class CommitMappingExtensions
{
    public static ModelRequest ToModel(this GrpcRequest request, DateTimeOffset timestampUtc)
    {
        return new ModelRequest(
            request.ConsumerId,
            request.ConsumerGroup,
            request.OwnershipsForProcessedPartitions
                .Select(partitionOwnership => partitionOwnership.ToModel())
                .ToArray(),
            request.ProcessedMessages
                .Select(message => message.ToModel())
                .ToArray(),
            request.RetryableMessages
                .Select(message => message.ToModel())
                .ToArray(),
            request.FailedMessages
                .Select(message => message.ToModel())
                .ToArray(),
            timestampUtc);
    }

    public static GrpcCommitResponse ToGrpc(this ModelCommitResponse response)
    {
        return new GrpcCommitResponse
        {
            CurrentOwnershipsForConsumerGroup = response.ConsumerGroupPartitionOwnerships.ToGrpc(),
            PartitionsWithIrrelevantOwnership =
            {
                response.PartitionsWithIrrelevantOwnership.Select(partition => partition.ToGrpc())
            }
        };
    }

    private static CommitedPartition ToModel(this PartitionOwnership partition)
    {
        return new CommitedPartition(
            partition.Topic,
            partition.Partition,
            partition.OwnerEpoch);
    }

    private static ModelProcessedMessage ToModel(this GrpcProcessedMessage processedMessage)
    {
        return new ModelProcessedMessage(
            processedMessage.Topic,
            processedMessage.Partition,
            processedMessage.Offset);
    }

    private static ModelRetryableMessage ToModel(this GrpcRetryableMessage retryableMessage)
    {
        return new ModelRetryableMessage(
            retryableMessage.Message.Topic,
            retryableMessage.Message.Partition,
            retryableMessage.Message.Offset,
            retryableMessage.Message.Headers.ToDictionary(
                header => header.Key,
                header => header.Value.ToByteArray()),
            retryableMessage.Message.Key?.ToByteArray(),
            retryableMessage.Message.Value?.ToByteArray(),
            retryableMessage.Message.Timestamp.ToDateTimeOffset(),
            retryableMessage.Message.MaxRetriesCount,
            retryableMessage.Message.RetriesCount,
            retryableMessage.Message.ProcessingDeadlineUtc?.ToDateTimeOffset(),
            retryableMessage.NextRetryAfterMs);
    }

    private static ModelFailedMessage ToModel(this GrpcFailedMessage failedMessage)
    {
        return new ModelFailedMessage(
            failedMessage.Message.Topic,
            failedMessage.Message.Partition,
            failedMessage.Message.Offset,
            failedMessage.Message.Headers.ToDictionary(
                header => header.Key,
                header => header.Value.ToByteArray()),
            failedMessage.Message.Key?.ToByteArray(),
            failedMessage.Message.Value?.ToByteArray(),
            failedMessage.Message.Timestamp.ToDateTimeOffset(),
            failedMessage.Message.RetriesCount,
            failedMessage.FailedAtUtc.ToDateTimeOffset());
    }

    private static GrpcPartitionWithIrrelevantOwnership ToGrpc(this ModelPartitionWithIrrelevantOwnership partition)
    {
        return new GrpcPartitionWithIrrelevantOwnership
        {
            Topic = partition.Topic,
            Partition = partition.Partition,
        };
    }
}