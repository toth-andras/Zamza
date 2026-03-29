using Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;
using Zamza.Server.DataAccess.Repositories.CommonModels;
using Zamza.Server.Models.ConsumerApi.Commit;
using Zamza.Server.Models.ConsumerApi.Fetch;

namespace Zamza.Server.DataAccess.Repositories.RetryQueueRepository;

public interface IRetryQueueRepository
{
    Task<List<FetchedMessage>> GetForFetch(
        string consumerGroup,
        IReadOnlyCollection<FetchedPartition> partitions,
        int limit,
        CancellationToken cancellationToken);
    
    Task Delete(
        IDbTransactionFrame transaction,
        string consumerGroup,
        IReadOnlyCollection<MessageToDeleteDto> messages,
        CancellationToken cancellationToken);
    
    Task Upsert(
        IDbTransactionFrame transaction,
        string consumerGroup,
        IReadOnlyCollection<RetryableMessage> messages,
        CancellationToken cancellationToken);
}