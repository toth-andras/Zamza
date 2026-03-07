using Zamza.Server.Application.ConsumerApi.Ping.Models;
using Zamza.Server.Models.ConsumerApi;

namespace Zamza.Server.Application.ConsumerApi.Ping;

public interface IPingService
{
    Task<PingResponse> Ping(PingRequest request, CancellationToken cancellationToken);
}