using Zamza.Server.Models.ConsumerApi;

namespace Zamza.Server.Application.ConsumerApi.Fetch.Models;

public sealed record FetchRequest(
    string? BearerToken,
    string ConsumerGroup,
    IReadOnlyList<PartitionFetch> PartitionFetches,
    int Limit);