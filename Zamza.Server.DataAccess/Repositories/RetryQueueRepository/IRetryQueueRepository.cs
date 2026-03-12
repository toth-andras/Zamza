using Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;
using Zamza.Server.Models.ConsumerApi;

namespace Zamza.Server.DataAccess.Repositories.RetryQueueRepository;

public interface IRetryQueueRepository
{
    /// <summary>
    /// Returns messages for ConsumerApi.Fetch flow.
    /// </summary>
    Task<List<ConsumerApiMessage>> GetForFetch(
        string consumerGroup,
        IReadOnlyList<PartitionFetch> partitions,
        int limit,
        CancellationToken cancellationToken);
    
    Task ClearMessages(
        IDbTransactionFrame transaction,
        MessageKeysSet messages,
        CancellationToken cancellationToken);
    
    Task Insert(
        IDbTransactionFrame transaction,
        string consumerGroup,
        IReadOnlyList<FailedMessage> messages,
        CancellationToken cancellationToken);
}