using System.Data;
using Zamza.Server.Application.ConsumerApi.Commit.Models;
using Zamza.Server.DataAccess.Common.ConnectionsManagement;
using Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;
using Zamza.Server.DataAccess.Repositories.CommonModels;
using Zamza.Server.DataAccess.Repositories.ConsumerHeartbeatRepository;
using Zamza.Server.DataAccess.Repositories.DLQRepository;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.Models;
using Zamza.Server.DataAccess.Repositories.RetryQueueRepository;
using Zamza.Server.Models.ConsumerApi.Commit;
using Zamza.Server.Models.ConsumerApi.Common;
using Zamza.Server.Models.ConsumerApi.Monitoring;
using Zamza.Server.Models.Exceptions;

namespace Zamza.Server.Application.ConsumerApi.Commit;

internal sealed class CommitService : ICommitService
{
    private readonly IDbConnectionsManager _dbConnectionsManager;
    private readonly IPartitionOwnershipRepository _partitionOwnershipRepository;
    private readonly IRetryQueueRepository _retryQueueRepository;
    private readonly IDLQRepository _dlqRepository;
    private readonly IConsumerHeartbeatRepository _consumerHeartbeatRepository;

    public CommitService(
        IDbConnectionsManager dbConnectionsManager,
        IPartitionOwnershipRepository partitionOwnershipRepository,
        IRetryQueueRepository retryQueueRepository,
        IDLQRepository dlqRepository)
    {
        _dbConnectionsManager = dbConnectionsManager;
        _partitionOwnershipRepository = partitionOwnershipRepository;
        _retryQueueRepository = retryQueueRepository;
        _dlqRepository = dlqRepository;
    }

    public async Task<CommitResponse> Commit(
        CommitRequest request,
        CancellationToken cancellationToken)
    {
        await SaveConsumerHeartbeat(request, cancellationToken);
        
        VerifyAllCommitedPartitionsAreStated(request);
        
        await using var transaction = await _dbConnectionsManager.BeginTransaction(
            IsolationLevel.ReadCommitted, 
            cancellationToken);
        
        await LockPartitions(
            transaction,
            request.ConsumerGroup,
            request.Partitions,
            cancellationToken);

        var consumerGroupPartitionOwnership = await _partitionOwnershipRepository.GetForConsumerGroup(
            transaction,
            request.ConsumerGroup,
            cancellationToken);

        var partitionsWithIrrelevantOwnership = GetPartitionsWithIrrelevantOwnership(
            request.Partitions,
            consumerGroupPartitionOwnership);
        
        var messagesFromPartitionsWithRelevantOwnership = FilterOutMessagesWithFromPartitionsWithIrrelevantOwnership(
            request,
            partitionsWithIrrelevantOwnership);
        
        await DeleteMessagesFromRetryQueue(
            transaction,
            request.ConsumerGroup,
            messagesFromPartitionsWithRelevantOwnership,
            cancellationToken);
        
        await DeleteMessagesFromDLQ(
            transaction,
            request.ConsumerGroup,
            messagesFromPartitionsWithRelevantOwnership,
            cancellationToken);

        await _retryQueueRepository.Upsert(
            transaction,
            request.ConsumerGroup,
            messagesFromPartitionsWithRelevantOwnership.RetryableMessages,
            cancellationToken);
        
        await _dlqRepository.Upsert(
            transaction,
            request.ConsumerGroup,
            messagesFromPartitionsWithRelevantOwnership.FailedMessages,
            cancellationToken);

        await transaction.Commit(cancellationToken);

        return new CommitResponse(
            consumerGroupPartitionOwnership,
            partitionsWithIrrelevantOwnership
                .Select(partition => new PartitionWithIrrelevantOwnership(partition.Topic, partition.Partition))
                .ToList());
    }

    private async Task SaveConsumerHeartbeat(
        CommitRequest request,
        CancellationToken cancellationToken)
    {
        var heartbeat = new ConsumerHeartbeat(
            request.ConsumerId,
            request.ConsumerGroup,
            request.TimstampUtc);
        
        await _consumerHeartbeatRepository.SaveHeartbeat(heartbeat, cancellationToken);
    }
    
    private static void VerifyAllCommitedPartitionsAreStated(CommitRequest request)
    {
        var statedPartitions = new HashSet<(string Topic, int Partition)>(request.Partitions.Count);
        statedPartitions.UnionWith(request.Partitions.Select(partition => (partition.Topic, partition.Partition)));

        foreach (var message in request.ProcessedMessages)
        {
            if (statedPartitions.Contains((message.Topic, message.Partition)) is false)
            {
                Throw();
            }
        }
        
        foreach (var message in request.RetryableMessages)
        {
            if (statedPartitions.Contains((message.Topic, message.Partition)) is false)
            {
                Throw();
            }
        }
        
        foreach (var message in request.FailedMessages)
        {
            if (statedPartitions.Contains((message.Topic, message.Partition)) is false)
            {
                Throw();
            }
        }

        return;

        void Throw()
        {
            throw new BadRequestException(
                "The request contains messages from partitions the ownership epoch was not provided for");
        }
    }

    private async Task LockPartitions(
        IDbTransactionFrame transaction,
        string consumerGroup,
        IReadOnlyCollection<CommitedPartition> partitions,
        CancellationToken cancellationToken)
    {
        var partitionsToLock = partitions
            .Select(partition => new PartitionToLock(partition.Topic, partition.Partition))
            .ToList();

        await _partitionOwnershipRepository.LockPartitions(
            transaction,
            consumerGroup,
            partitionsToLock,
            cancellationToken);
    }

    private async Task DeleteMessagesFromRetryQueue(
        IDbTransactionFrame transaction,
        string consumerGroup,
        MessagesFromPartitionsWithRelevantOwnership messages,
        CancellationToken cancellationToken)
    {
        var messagesToDelete = messages.ProcessedMessages
            .Select(message => new MessageToDelete(message.Topic, message.Partition, message.Offset))
            .Concat
            (
                messages.FailedMessages
                    .Select(message => new MessageToDelete(message.Topic, message.Partition, message.Offset))
            )
            .ToList();
        
        await _retryQueueRepository.Delete(
            transaction,
            consumerGroup,
            messagesToDelete,
            cancellationToken);
    }

    private async Task DeleteMessagesFromDLQ(
        IDbTransactionFrame transaction,
        string consumerGroup,
        MessagesFromPartitionsWithRelevantOwnership messages,
        CancellationToken cancellationToken)
    {
        var messagesToDelete = messages.ProcessedMessages
            .Select(message => new MessageToDelete(message.Topic, message.Partition, message.Offset))
            .Concat
            (
                messages.RetryableMessages
                    .Select(message => new MessageToDelete(message.Topic, message.Partition, message.Offset))
            )
            .ToList();
        
        await _dlqRepository.Delete(
            transaction,
            consumerGroup,
            messagesToDelete,
            cancellationToken);
    }

    private static HashSet<(string Topic, int Partition)> GetPartitionsWithIrrelevantOwnership(
        IReadOnlyCollection<CommitedPartition> partitionsStatedForCommit,
        ConsumerGroupPartitionOwnershipSet consumerGroupPartitionOwnerships)
    {
        var partitionsWithIrrelevantOwnership = new HashSet<(string Topic, int Partition)>();
        
        foreach (var statedPartition in partitionsStatedForCommit)
        {
            var partitionNotRegistered = consumerGroupPartitionOwnerships.IsPartitionRegistered(
                statedPartition.Topic,
                statedPartition.Partition) is false;
            
            var currentPartitionOwnerEpoch = consumerGroupPartitionOwnerships.GetOwnerEpochForPartition(
                statedPartition.Topic,
                statedPartition.Partition);
            
            var partitionOwnershipObsolete = statedPartition.OwnershipEpoch != currentPartitionOwnerEpoch;

            if (partitionNotRegistered || partitionOwnershipObsolete)
            {
                partitionsWithIrrelevantOwnership.Add((statedPartition.Topic, statedPartition.Partition));
            }
        }
        
        return partitionsWithIrrelevantOwnership;
    }

    private static MessagesFromPartitionsWithRelevantOwnership FilterOutMessagesWithFromPartitionsWithIrrelevantOwnership(
        CommitRequest request,
        IReadOnlySet<(string Topic, int Partition)> partitionsWithIrrelevantOwnership)
    {
        return new MessagesFromPartitionsWithRelevantOwnership(
            request.ProcessedMessages
                .Where(message => partitionsWithIrrelevantOwnership.Contains((message.Topic, message.Partition)) is false)
                .ToList(),
            request.RetryableMessages
                .Where(message => partitionsWithIrrelevantOwnership.Contains((message.Topic, message.Partition)) is false)
                .ToList(),
            request.FailedMessages
                .Where(message => partitionsWithIrrelevantOwnership.Contains((message.Topic, message.Partition)) is false)
                .ToList());
    }
    
    private sealed record MessagesFromPartitionsWithRelevantOwnership(
        IReadOnlyCollection<ProcessedMessage> ProcessedMessages,
        IReadOnlyCollection<RetryableMessage> RetryableMessages,
        IReadOnlyCollection<FailedMessage> FailedMessages);
}