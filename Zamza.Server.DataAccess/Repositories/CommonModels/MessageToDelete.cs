namespace Zamza.Server.DataAccess.Repositories.CommonModels;

public sealed record MessageToDelete(
    string Topic,
    int Partition,
    long Offset);