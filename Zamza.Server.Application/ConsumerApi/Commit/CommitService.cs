using System.Data;
using Zamza.Server.Application.ConsumerApi.Commit.Models;
using Zamza.Server.DataAccess.Common.ConnectionsManagement;
using Zamza.Server.DataAccess.Repositories.DlqRepository;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository;
using Zamza.Server.DataAccess.Repositories.RetryQueueRepository;
using Zamza.Server.Models.ConsumerApi;

namespace Zamza.Server.Application.ConsumerApi.Commit;

internal sealed class CommitService : ICommitService
{
    private readonly IDbConnectionsManager  _dbConnectionsManager;
    private readonly IPartitionOwnershipRepository _partitionOwnershipRepository;
    private readonly IRetryQueueRepository _retryQueueRepository;
    private readonly IDlqRepository  _dlqRepository;

    public CommitService(
        IDbConnectionsManager dbConnectionsManager,
        IPartitionOwnershipRepository partitionOwnershipRepository,
        IRetryQueueRepository retryQueueRepository,
        IDlqRepository dlqRepository)
    {
        _partitionOwnershipRepository = partitionOwnershipRepository;
        _retryQueueRepository = retryQueueRepository;
        _dlqRepository = dlqRepository;
        _dbConnectionsManager = dbConnectionsManager;
    }

    public async Task<CommitResponse> Commit(
        CommitRequest request,
        CancellationToken cancellationToken)
    {
        await using var transaction = await _dbConnectionsManager.BeginTransaction(
            IsolationLevel.ReadCommitted,
            cancellationToken);

        await _partitionOwnershipRepository.LockPartitions(
            transaction,
            request.ConsumerPartitionOwnerships,
            cancellationToken);

        var (ownedPartitions, currentOwnership) = await GetOwnedPartitions(
            request.ConsumerPartitionOwnerships,
            cancellationToken);

        var messagesFromUnownedPartitions = request.ProcessedMessages.TopicValue
            .Select((_, index) => new TopicPartitionOffset(
                request.ProcessedMessages.TopicValue[index],
                request.ProcessedMessages.PartitionValue[index],
                request.ProcessedMessages.OffsetValue[index]))
            .Where(tpo => ownedPartitions.Contains((tpo.Topic, tpo.Partition)) is false)
            .Concat(
                request.FailedMessages
                    .Where(message => ownedPartitions.Contains((message.Message.Topic, message.Message.Partition)) is false)
                    .Select(message => new TopicPartitionOffset(message.Message.Topic, message.Message.Partition, message.Message.Offset))
            )
            .Concat(
                request.PoisonedMessages
                    .Where(message => ownedPartitions.Contains((message.Topic, message.Partition)) is false)
                    .Select(message => new TopicPartitionOffset(message.Topic, message.Partition, message.Offset))
            )
            .ToList();

        request = RemoveUnownedPartitionsMessages(request, ownedPartitions);

        var (toRemoveFromRetryQueue, toRemoveFromDql) = GetMessagesToRemove(request);

        await _retryQueueRepository.ClearMessages(
            transaction,
            toRemoveFromRetryQueue,
            cancellationToken);

        await _dlqRepository.ClearMessages(
            transaction,
            toRemoveFromDql,
            cancellationToken);

        await _retryQueueRepository.Insert(
            transaction,
            request.ConsumerGroup,
            request.FailedMessages,
            cancellationToken);

        await _dlqRepository.Insert(
            transaction,
            request.ConsumerGroup,
            request.PoisonedMessages,
            cancellationToken);
        
        await transaction.Commit(cancellationToken);

        return new CommitResponse(
            currentOwnership,
            messagesFromUnownedPartitions,
            []);
    }

    private async Task<
        (HashSet<(string Topic, int Partition)> OwnedPartitions,
        IReadOnlyCollection<PartitionOwnership> CurrentPartitionOwnership)> GetOwnedPartitions(
        ConsumerPartitionOwnershipsSet consumerPartitionOwnerships,
        CancellationToken cancellationToken)
    {
        var ownershipsFromDb = await _partitionOwnershipRepository.Get(
            consumerPartitionOwnerships.ConsumerGroup,
            cancellationToken);
        
        var ownedPartitions = consumerPartitionOwnerships.ConsumerPartitionOwnerships
            .Where(consumerOwnership => 
                ownershipsFromDb.TryGetValue(consumerOwnership.Key, out var dbOwnership) 
                && dbOwnership.Epoch == consumerOwnership.Value)
            .Select(consumerOwnership => consumerOwnership.Key)
            .ToHashSet();
        
        return (ownedPartitions, ownershipsFromDb.Values.ToList());
    }

    private static CommitRequest RemoveUnownedPartitionsMessages(
        CommitRequest initialRequest,
        IReadOnlySet<(string Topic, int Partition)> ownedPartitions)
    {
        var topics = new List<string>(initialRequest.ProcessedMessages.MessageCount);
        var partitions = new List<int>(initialRequest.ProcessedMessages.MessageCount);
        var offsets = new List<long>(initialRequest.ProcessedMessages.MessageCount);
        for (int i = 0; i < initialRequest.ProcessedMessages.MessageCount; i++)
        {
            if (ownedPartitions.Contains((initialRequest.ProcessedMessages.TopicValue[i],
                    initialRequest.ProcessedMessages.PartitionValue[i])))
            {
                topics.Add(initialRequest.ProcessedMessages.TopicValue[i]);
                partitions.Add(initialRequest.ProcessedMessages.PartitionValue[i]);
                offsets.Add(initialRequest.ProcessedMessages.OffsetValue[i]);
            }
        }
        
        return new CommitRequest(
            initialRequest.ConsumerId,
            initialRequest.ConsumerGroup,
            new MessageKeysSet(
                initialRequest.ConsumerGroup,
                topics.ToArray(),
                partitions.ToArray(),
                offsets.ToArray()),
            initialRequest.FailedMessages
                .Where(message => ownedPartitions.Contains((message.Message.Topic, message.Message.Partition)))
                .ToList(),
            initialRequest.PoisonedMessages
                .Where(message => ownedPartitions.Contains((message.Topic, message.Partition)))
                .ToList(),
            initialRequest.ConsumerPartitionOwnerships
        );
    }

    private static (MessageKeysSet RemoveFromRetryQueue, MessageKeysSet RemoveFromDlq) GetMessagesToRemove(
        CommitRequest request)
    {
        var toRemoveFromRetryQueueTopics = request.ProcessedMessages.TopicValue
            .Concat(request.PoisonedMessages.Select(message => message.Topic))
            .ToArray();

        var toRemoveFromRetryQueuePartitions = request.ProcessedMessages.PartitionValue
            .Concat(request.PoisonedMessages.Select(message => message.Partition))
            .ToArray();

        var toRemoveFromRetryQueueOffsets = request.ProcessedMessages.OffsetValue
            .Concat(request.PoisonedMessages.Select(message => message.Offset))
            .ToArray();

        var toRemoveFromDlqTopics = request.ProcessedMessages.TopicValue
            .Concat(request.FailedMessages.Select(message => message.Message.Topic))
            .ToArray();

        var toRemoveFromDlqPartitions = request.ProcessedMessages.PartitionValue
            .Concat(request.PoisonedMessages.Select(message => message.Partition))
            .ToArray();
        
        var toRemoveFromDlqOffsets = request.ProcessedMessages.OffsetValue
            .Concat(request.PoisonedMessages.Select(message => message.Offset))
            .ToArray();
        
        return (
            new MessageKeysSet(
                request.ConsumerGroup,
                toRemoveFromRetryQueueTopics,
                toRemoveFromRetryQueuePartitions,
                toRemoveFromRetryQueueOffsets),
            new MessageKeysSet(
                request.ConsumerGroup,
                toRemoveFromDlqTopics,
                toRemoveFromDlqPartitions,
                toRemoveFromDlqOffsets));
    }
}