using Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.Models;
using Zamza.Server.Models.ConsumerApi;

namespace Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository;

public interface IPartitionOwnershipRepository
{
    Task<IReadOnlyDictionary<(string Topic, int Partition), PartitionOwnership>> Get(
        string consumerGroup, 
        CancellationToken cancellation);
    
    Task<IReadOnlyDictionary<(string Topic, int Partition), PartitionOwnership>> Get(
        string consumerGroup, 
        IDbTransactionFrame transaction,
        CancellationToken cancellation);

    Task<CheckPartitionsOwnershipsRelevanceResponse> CheckPartitionsOwnershipsRelevance(
        string consumerGroup,
        IReadOnlyCollection<PartitionFetch> fetchesToCheck,
        CancellationToken cancellation);
    
    Task LockPartitions(
        IDbTransactionFrame transaction,
        ConsumerPartitionOwnershipsSet partitionOwnerships,
        CancellationToken cancellationToken);

    Task StopConsumerLeaderships(
        string consumerId,
        string consumerGroup,
        CancellationToken cancellation);

    Task LockPartitions(
        IDbTransactionFrame transaction,
        PartitionOwnershipClaimsSet partitionsSource,
        CancellationToken cancellationToken);

    Task Insert(
        IDbTransactionFrame transaction,
        string consumerGroup,
        int ownershipsCount,
        IEnumerable<PartitionOwnership> consumerGroupOwnerships,
        CancellationToken cancellation);
}