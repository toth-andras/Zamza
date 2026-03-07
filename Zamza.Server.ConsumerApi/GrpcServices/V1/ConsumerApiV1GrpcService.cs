using Grpc.Core;
using Zamza.ConsumerApi.V1;
using Zamza.Server.Application.ConsumerApi.Fetch;
using Zamza.Server.ConsumerApi.GrpcServices.V1.Mapping;
using Zamza.Server.ConsumerApi.Utils;

namespace Zamza.Server.ConsumerApi.GrpcServices.V1;

internal sealed class ConsumerApiV1GrpcService : ConsumerApiV1.ConsumerApiV1Base
{
    private readonly IFetchService _fetchService;

    public ConsumerApiV1GrpcService(IFetchService fetchService)
    {
        _fetchService = fetchService;
    }

    public override async Task<FetchResponse> Fetch(
        FetchRequest request, 
        ServerCallContext context)
    {
        var bearerToken = BearerTokenHelper.GetBearerToken(context.RequestHeaders);
        
        var result = await _fetchService.Fetch(
            request.ToModel(bearerToken),
            context.CancellationToken);

        return result.ToGrpc();
    }
}