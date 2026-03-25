using Zamza.Server.Application.ConsumerApi.Fetch.Models;

namespace Zamza.Server.Application.ConsumerApi.Fetch;

public interface IFetchService
{
    Task<FetchResponse> Fetch(FetchRequest request, CancellationToken cancellationToken);
}