using Zamza.Server.Models.ConsumerApi;
using GrpcClaimRequest = Zamza.ConsumerApi.V1.ClaimPartitionOwnershipRequest;
using ModelClaimRequest = Zamza.Server.Application.ConsumerApi.ClaimPartitionOwnership.Models.ClaimPartitionOwnershipRequest;
using GrpcClaimResponse = Zamza.ConsumerApi.V1.ClaimPartitionOwnershipResponse;
using ModelClaimResponse = Zamza.Server.Application.ConsumerApi.ClaimPartitionOwnership.Models.ClaimPartitionOwnershipResponse;

namespace Zamza.Server.ConsumerApi.GrpcServices.V1.Mapping;

internal static class ClaimPartitionOwnershipMappingExtensions
{
    public static ModelClaimRequest ToModel(
        this GrpcClaimRequest request,
        string? bearerToken)
    {
        var topics = new string[request.Claims.Count];
        var partitions = new int[request.Claims.Count];
        var epochs = new long[request.Claims.Count];

        for (var i = 0; i < request.Claims.Count; i++)
        {
            topics[i] = request.Claims[i].Topic;
            partitions[i] = request.Claims[i].Partition;
            epochs[i] = request.Claims[i].KnownOwnershipEpoch;
        }
        
        var claims = new PartitionOwnershipClaimsSet(
            request.ConsumerId,
            request.ConsumerGroup,
            topics,
            partitions,
            epochs);

        return new ModelClaimRequest(bearerToken, claims);
    }

    public static GrpcClaimResponse ToGrpc(
        this ModelClaimResponse response,
        string consumerId)
    {
        return response.Result.Match(
            _ => ToOwnershipObsoleteResponse(response.CurrentPartitionOwnershipsForConsumerGroup),
            permissionDenied => ToPermissionDeniedResponse(
                response.CurrentPartitionOwnershipsForConsumerGroup,
                permissionDenied.ProhibitedTopics),
            _ => ToOkResponse(response.CurrentPartitionOwnershipsForConsumerGroup, consumerId));
    }

    private static GrpcClaimResponse ToOwnershipObsoleteResponse(
        IEnumerable<PartitionOwnership> currentOwnerships)
    {
        return new GrpcClaimResponse
        {
            CurrentPartitionOwners =
            {
                currentOwnerships.Select(ownership => ownership.ToGrpc())
            },
            PartitionOwnershipObsolete = new GrpcClaimResponse.Types.ClaimPartitionOwnershipOneOfPartitionOwnershipObsolete()
        };
    }

    private static GrpcClaimResponse ToPermissionDeniedResponse(
        IEnumerable<PartitionOwnership> currentOwnerships,
        IReadOnlyCollection<string> prohibitedTopics)
    {
        return new GrpcClaimResponse
        {
            CurrentPartitionOwners =
            {
                currentOwnerships.Select(ownership => ownership.ToGrpc())
            },
            PermissionDenied = new GrpcClaimResponse.Types.ClaimPartitionOwnershipOneOfPermissionDenied
            {
                ProhibitedTopics = {prohibitedTopics}
            }
        };
    }

    private static GrpcClaimResponse ToOkResponse(
        IEnumerable<PartitionOwnership> currentOwnerships,
        string consumerId)
    {

        var ownerships = currentOwnerships.ToList();
        return new GrpcClaimResponse
        {
            CurrentPartitionOwners =
            {
                ownerships.Select(ownership => ownership.ToGrpc())
            },
            Ok = new GrpcClaimResponse.Types.ClaimPartitionOwnershipOneOfOk
            {
                OwnedPartitions =
                {
                    ownerships
                        .Where(ownership => consumerId.Equals(ownership.ConsumerId))
                        .Select(ownership => new GrpcClaimResponse.Types.ClaimPartitionOwnershipOneOfOk.Types.GrantedPartitionOwnership
                        {
                            Topic = ownership.Topic,
                            Partition = ownership.Partition,
                            OwnershipEpoch = ownership.Epoch
                        })
                }
            }
        };
    }
}