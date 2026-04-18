using Grpc.Core;
using Zamza.ConsumerApi.V1;
using Zamza.Server.Application.ConsumerApi.ClaimPartitionOwnership;
using Zamza.Server.Application.ConsumerApi.Commit;
using Zamza.Server.Application.ConsumerApi.Fetch;
using Zamza.Server.Application.ConsumerApi.Leave;
using Zamza.Server.ConsumerApi.GrpcServices.V1.Mapping;
using Zamza.Server.ConsumerApi.Utils.DateTimeProvider;

namespace Zamza.Server.ConsumerApi.GrpcServices.V1;

public sealed class ConsumerApiGrpcService : ConsumerApiV1.ConsumerApiV1Base
{
    private readonly IClaimPartitionOwnershipService _claimPartitionOwnershipService;
    private readonly IFetchService _fetchService;
    private readonly ICommitService _commitService;
    private readonly ILeaveService _leaveService;
    private readonly IDateTimeProvider _dateTimeProvider;

    public ConsumerApiGrpcService(
        IClaimPartitionOwnershipService claimPartitionOwnershipService,
        IFetchService fetchService,
        ICommitService commitService,
        ILeaveService leaveService,
        IDateTimeProvider dateTimeProvider)
    {
        _claimPartitionOwnershipService = claimPartitionOwnershipService;
        _fetchService = fetchService;
        _commitService = commitService;
        _leaveService = leaveService;
        _dateTimeProvider = dateTimeProvider;
    }

    public override async Task<ClaimPartitionOwnershipResponse> ClaimPartitionOwnership(
        ClaimPartitionOwnershipRequest request,
        ServerCallContext context)
    {
        var requestModel = request.ToModel(_dateTimeProvider.GetUtcNow());

        var responseModel = await _claimPartitionOwnershipService.ClaimPartitionOwnership(
            requestModel,
            context.CancellationToken);

        return responseModel.ToGrpc();
    }

    public override async Task<FetchResponse> Fetch(
        FetchRequest request,
        ServerCallContext context)
    {
        var requestModel = request.ToModel();
        
        var responseModel = await _fetchService.Fetch(requestModel, context.CancellationToken);

        return responseModel.ToGrpc();
    }

    public override async Task<CommitResponse> Commit(
        CommitRequest request,
        ServerCallContext context)
    {
        var requestModel = request.ToModel(_dateTimeProvider.GetUtcNow());
        
        var responseModel = await _commitService.Commit(requestModel, context.CancellationToken);

        return responseModel.ToGrpc();
    }

    public override async Task<LeaveResponse> Leave(
        LeaveRequest request,
        ServerCallContext context)
    {
        var requestModel = request.ToModel(_dateTimeProvider.GetUtcNow());
        
        await _leaveService.Leave(requestModel, context.CancellationToken);
        
        return new LeaveResponse();
    }

    public override Task<PingResponse> Ping(
        PingRequest request,
        ServerCallContext context)
    {
        // Doing nothing, as this request is only used to determine
        // the availability of the service
        
        return Task.FromResult(new PingResponse());
    }
}