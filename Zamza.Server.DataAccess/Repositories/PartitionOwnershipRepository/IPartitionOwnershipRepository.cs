using Zamza.Server.Models.ConsumerApi;

namespace Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository;

public interface IPartitionOwnershipRepository
{
    Task<IReadOnlyDictionary<(string Topic, int Partition), PartitionOwnership>> Get(
        string consumerGroup, 
        CancellationToken cancellation);
}