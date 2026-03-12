using Grpc.Core;
using Zamza.ConsumerApi.V1;
using Zamza.Server.Application.ConsumerApi.Commit;
using Zamza.Server.Application.ConsumerApi.Fetch;
using Zamza.Server.Application.ConsumerApi.Ping;
using Zamza.Server.ConsumerApi.GrpcServices.V1.Mapping;
using Zamza.Server.ConsumerApi.Utils;

namespace Zamza.Server.ConsumerApi.GrpcServices.V1;

internal sealed class ConsumerApiV1GrpcService : ConsumerApiV1.ConsumerApiV1Base
{
    private readonly IFetchService _fetchService;
    private readonly IPingService _pingService;
    private readonly ICommitService _commitService;

    public ConsumerApiV1GrpcService(
        IFetchService fetchService,
        IPingService pingService, 
        ICommitService commitService)
    {
        _fetchService = fetchService;
        _pingService = pingService;
        _commitService = commitService;
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

    public override async Task<CommitResponse> Commit(
        CommitRequest request,
        ServerCallContext context)
    {
        var bearerToken = BearerTokenHelper.GetBearerToken(context.RequestHeaders);

        var result = await _commitService.Commit(
            request.ToModel(),
            context.CancellationToken);

        return result.ToGrpc();
    }

    public override async Task<PingResponse> Ping(
        PingRequest request, 
        ServerCallContext context)
    {
        var result = await _pingService.Ping(
            request.ToModel(),
            context.CancellationToken);

        return result.ToGrpc();
    }
}