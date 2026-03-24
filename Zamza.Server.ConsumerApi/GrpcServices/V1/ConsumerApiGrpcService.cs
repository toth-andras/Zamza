using Grpc.Core;
using Zamza.ConsumerApi.V1;
using Zamza.Server.Application.ConsumerApi.ClaimPartitionOwnership;
using Zamza.Server.ConsumerApi.GrpcServices.V1.Mapping;
using Zamza.Server.ConsumerApi.Utils.DateTimeProvider;

namespace Zamza.Server.ConsumerApi.GrpcServices.V1;

public sealed class ConsumerApiGrpcService : ConsumerApiV1.ConsumerApiV1Base
{
    private readonly IClaimPartitionOwnershipService _claimPartitionOwnershipService;
    private readonly IDateTimeProvider _dateTimeProvider;

    public ConsumerApiGrpcService(
        IClaimPartitionOwnershipService claimPartitionOwnershipService,
        IDateTimeProvider dateTimeProvider)
    {
        _claimPartitionOwnershipService = claimPartitionOwnershipService;
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
}