namespace Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.Models;

public sealed record PartitionToLock(
    string Topic,
    int Partition);