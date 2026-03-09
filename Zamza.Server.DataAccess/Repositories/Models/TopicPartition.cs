namespace Zamza.Server.DataAccess.Repositories.Models;

public sealed record TopicPartition(
    string Topic,
    int Partition);