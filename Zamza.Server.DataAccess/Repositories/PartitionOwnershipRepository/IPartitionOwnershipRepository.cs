using Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.Models;
using Zamza.Server.Models.ConsumerApi;

namespace Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository;

public interface IPartitionOwnershipRepository
{
    Task<IReadOnlyDictionary<(string Topic, int Partition), PartitionOwnership>> Get(
        string consumerGroup, 
        CancellationToken cancellation);

    Task<CheckPartitionsOwnershipsRelevanceResponse> CheckPartitionsOwnershipsRelevance(
        string consumerGroup,
        IReadOnlyCollection<PartitionFetch> fetchesToCheck,
        CancellationToken cancellation);

    Task<CheckPartitionsOwnershipsRelevanceResponse> CheckPartitionsOwnershipsRelevance(
        ConsumerPartitionOwnershipsSet ownershipsToCheck,
        CancellationToken cancellationToken);
    
    Task LockPartitions(
        IDbTransactionFrame transaction,
        ConsumerPartitionOwnershipsSet partitionOwnerships,
        CancellationToken cancellationToken);
}