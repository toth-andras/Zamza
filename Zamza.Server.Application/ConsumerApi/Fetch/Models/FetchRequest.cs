using Zamza.Server.Models.ConsumerApi.Fetch;

namespace Zamza.Server.Application.ConsumerApi.Fetch.Models;

public sealed record FetchRequest(
    string ConsumerId,
    string ConsumerGroup,
    IReadOnlyCollection<FetchedPartition> Partitions,
    int Limit);