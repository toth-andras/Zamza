using Grpc.Core;
using Zamza.ConsumerApi.V1;
using Zamza.Server.Application.ConsumerApi.ClaimPartitionOwnership;
using Zamza.Server.Application.ConsumerApi.Commit;
using Zamza.Server.Application.ConsumerApi.Fetch;
using Zamza.Server.Application.ConsumerApi.Leave;
using Zamza.Server.Application.ConsumerApi.Ping;
using Zamza.Server.ConsumerApi.GrpcServices.V1.Mapping;
using Zamza.Server.ConsumerApi.Utils;

namespace Zamza.Server.ConsumerApi.GrpcServices.V1;

internal sealed class ConsumerApiV1GrpcService : ConsumerApiV1.ConsumerApiV1Base
{
    private readonly IFetchService _fetchService;
    private readonly ICommitService _commitService;
    private readonly IClaimPartitionOwnershipService _claimPartitionOwnershipService;
    private readonly IPingService _pingService;
    private readonly ILeaveService _leaveService;

    public ConsumerApiV1GrpcService(
        IFetchService fetchService,
        IClaimPartitionOwnershipService claimPartitionOwnershipService,
        ICommitService commitService,
        IPingService pingService, 
        ILeaveService leaveService)
    {
        _fetchService = fetchService;
        _pingService = pingService;
        _commitService = commitService;
        _leaveService = leaveService;
        _claimPartitionOwnershipService = claimPartitionOwnershipService;
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

    public override async Task<ClaimPartitionOwnershipResponse> ClaimPartitionOwnership(
        ClaimPartitionOwnershipRequest request,
        ServerCallContext context)
    {
        var bearerToken = BearerTokenHelper.GetBearerToken(context.RequestHeaders);

        var result = await _claimPartitionOwnershipService.ClaimPartitionOwnership(
            request.ToModel(bearerToken),
            context.CancellationToken);

        return result.ToGrpc(request.ConsumerId);
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

    public override async Task<LeaveResponse> Leave(
        LeaveRequest request,
        ServerCallContext context)
    {
        await _leaveService.Leave(
            request.ToModel(),
            context.CancellationToken);

        return new LeaveResponse();
    }
}