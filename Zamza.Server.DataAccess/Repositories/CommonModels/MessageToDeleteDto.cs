namespace Zamza.Server.DataAccess.Repositories.CommonModels;

public sealed record MessageToDeleteDto(
    string Topic,
    int Partition,
    long Offset);