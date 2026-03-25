using Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.Models;
using Zamza.Server.Models.ConsumerApi.Common;

namespace Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository;

public interface IPartitionOwnershipRepository
{
    Task LockPartitions(
        IDbTransactionFrame transaction,
        string consumerGroup,
        IReadOnlyList<PartitionToLock> claimedPartitions,
        CancellationToken cancellationToken);

    Task<ConsumerGroupPartitionOwnershipSet> GetForConsumerGroup(
        IDbTransactionFrame transaction,
        string consumerGroup,
        CancellationToken cancellationToken);
    
    Task<ConsumerGroupPartitionOwnershipSet> GetForConsumerGroup(
        string consumerGroup,
        CancellationToken cancellationToken);

    Task Upsert(
        IDbTransactionFrame transaction,
        ConsumerGroupPartitionOwnershipSet partitionOwnerships,
        CancellationToken cancellationToken);
}