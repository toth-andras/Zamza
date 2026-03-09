namespace Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.Models;

public sealed record PartitionLockInfo(
    string Topic,
    int Partition);