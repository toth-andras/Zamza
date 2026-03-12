using Zamza.ConsumerApi.V1;
using Zamza.Server.Models.ConsumerApi;
using ModelCommitRequest = Zamza.Server.Application.ConsumerApi.Commit.Models.CommitRequest;
using GrpcCommitRequest = Zamza.ConsumerApi.V1.CommitRequest;
using ModelFailedMessage = Zamza.Server.Models.ConsumerApi.FailedMessage;
using GrpcFailedMessage = Zamza.ConsumerApi.V1.CommitRequest.Types.FailedMessage;
using ModelCommitResponse = Zamza.Server.Application.ConsumerApi.Commit.Models.CommitResponse;
using GrpcCommitResponse = Zamza.ConsumerApi.V1.CommitResponse;

namespace Zamza.Server.ConsumerApi.GrpcServices.V1.Mapping;

internal static class CommitMappingExtensions
{
    public static ModelCommitRequest ToModel(this GrpcCommitRequest commitRequest)
    {
        return new ModelCommitRequest(
            commitRequest.ConsumerId,
            commitRequest.ConsumerGroup,
            ToMessageKeysSet(commitRequest.ProcessedMessages, commitRequest.ConsumerGroup),
            commitRequest.FailedMessages.Select(message => message.ToModel()).ToArray(),
            commitRequest.PoisonedMessages.Select(message => message.ToModel()).ToArray(),
            ToConsumerPartitionOwnershipsSet(commitRequest.OwnedPartitions, commitRequest.ConsumerGroup));
    }

    public static GrpcCommitResponse ToGrpc(this ModelCommitResponse modelCommitResponse)
    {
        return new GrpcCommitResponse
        {
            CurrentPartitionOwners =
            {
                modelCommitResponse.CurrentOwners.Select(ownership => ownership.ToGrpc())
            },
            UnownedPartitionsMessages =
            {
                modelCommitResponse.MessagesOfUnownedPartitions.Select(message => message.ToUnownedPartitionMessage())
            },
            ProhibitedTopicsMessages =
            {
                modelCommitResponse.MessagesOfProhibitedTopics.Select(message => message.ToProhibitedTopicMessage())
            }
        };
    }

    private static ModelFailedMessage ToModel(this GrpcFailedMessage message)
    {
        return new ModelFailedMessage(
            message.Message.ToModel(),
            message.NextRetryAfterMs);
    }

    private static MessageKeysSet ToMessageKeysSet(
        IReadOnlyCollection<GrpcCommitRequest.Types.ProcessedMessage> messages, 
        string consumerGroup)
    {
        return new MessageKeysSet(
            consumerGroup,
            messages.Select(message => message.Topic).ToArray(),
            messages.Select(message => message.Partition).ToArray(),
            messages.Select(message => message.Offset).ToArray());
    }

    private static ConsumerPartitionOwnershipsSet ToConsumerPartitionOwnershipsSet(
        IReadOnlyCollection<ConsumerPartitionOwnershipEpoch> ownerships,
        string consumerGroup)
    {
        var ownershipDictionary = new Dictionary<(string Topic, int Partition), long>(ownerships.Count);
        foreach (var ownership in ownerships)
        {
            ownershipDictionary[(ownership.Topic, ownership.Partition)] = ownership.OwnershipEpoch;
        }
        
        return new ConsumerPartitionOwnershipsSet(
            consumerGroup,
            ownershipDictionary);
    }

    private static CommitResponse.Types.MessageOfUnownedPartition ToUnownedPartitionMessage(
        this TopicPartitionOffset message)
    {
        return new CommitResponse.Types.MessageOfUnownedPartition
        {
            Topic = message.Topic,
            Partition = message.Partition,
            Offset = message.Offset,
        };
    }

    private static CommitResponse.Types.MessageOfProhibitedTopic ToProhibitedTopicMessage(
        this TopicPartitionOffset message)
    {
        return new CommitResponse.Types.MessageOfProhibitedTopic
        {
            Topic = message.Topic,
            Partition = message.Partition,
            Offset = message.Offset,
        };
    }
}