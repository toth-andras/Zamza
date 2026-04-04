namespace Zamza.Server.Application.ConsumerApi.Commit.Models;

public sealed record PartitionWithIrrelevantOwnership(
    string Topic,
    int Partition);