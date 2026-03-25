using Zamza.Server.Models.ConsumerApi.Fetch;

namespace Zamza.Server.DataAccess.Repositories.RetryQueueRepository;

public interface IRetryQueueRepository
{
    Task<List<FetchedMessage>> GetForFetch(
        string consumerGroup,
        IReadOnlyCollection<FetchedPartition> partitions,
        int limit,
        CancellationToken cancellationToken);
}