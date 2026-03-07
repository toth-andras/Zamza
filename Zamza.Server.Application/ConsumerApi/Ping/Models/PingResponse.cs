using Zamza.Server.Models.ConsumerApi;

namespace Zamza.Server.Application.ConsumerApi.Ping.Models;

public sealed record PingResponse(IEnumerable<PartitionOwnership> CurrentPartitionOwners);