using ModelFetchRequest = Zamza.Server.Application.ConsumerApi.Fetch.Models.FetchRequest;
using GrpcFetchRequest = Zamza.ConsumerApi.V1.FetchRequest;
using ModelPartitionFetch =  Zamza.Server.Models.ConsumerApi.PartitionFetch;
using GrpcPartitionFetch = Zamza.ConsumerApi.V1.FetchRequest.Types.PartitionFetch;
using ModelFetchResponse = Zamza.Server.Application.ConsumerApi.Fetch.Models.FetchResponse;
using GrpcFetchResponse = Zamza.ConsumerApi.V1.FetchResponse;
using GrpcCurrentPartitionOwner = Zamza.ConsumerApi.V1.CurrentPartitionOwner;

namespace Zamza.Server.ConsumerApi.GrpcServices.V1.Mapping;

internal static class FetchRequestMappingExtensions
{
    public static ModelFetchRequest ToModel(this GrpcFetchRequest request, string? bearerToken)
    {
        return new ModelFetchRequest(
            bearerToken,
            request.ConsumerGroup,
            request.Partitions.Select(fetch => fetch.ToModel()).ToList(),
            request.Limit);
    }

    public static GrpcFetchResponse ToGrpc(this ModelFetchResponse response)
    {
        var currentOwners = response.CurrentOwners
            .Select(owner => owner.ToGrpc());

        return response.Result.Match(
            _ => ToPartitionOwnershipObsolete(currentOwners),
            permissionDenied => ToPermissionDenied(currentOwners, permissionDenied),
            ok => ToOk(currentOwners, ok));
    }

    private static ModelPartitionFetch ToModel(this GrpcPartitionFetch partitionFetch)
    {
        return new ModelPartitionFetch(
            partitionFetch.Topic,
            partitionFetch.Partition,
            partitionFetch.KafkaOffset,
            partitionFetch.OwnershipEpoch);
    }

    private static GrpcFetchResponse ToPartitionOwnershipObsolete(IEnumerable<GrpcCurrentPartitionOwner> partitionOwners)
    {
        return new GrpcFetchResponse
        {
            CurrentPartitionOwners = {partitionOwners},
            PartitionOwnershipObsolete = new GrpcFetchResponse.Types.FetchResponseOneOfPartitionOwnershipObsolete()
        };
    }

    private static GrpcFetchResponse ToPermissionDenied(
        IEnumerable<GrpcCurrentPartitionOwner> partitionOwners,
        ModelFetchResponse.PermissionDenied value)
    {
        return new GrpcFetchResponse
        {
            CurrentPartitionOwners = {partitionOwners},
            PermissionDenied = new GrpcFetchResponse.Types.FetchResponseOneOfPermissionDenied
            {
                ProhibitedTopics = {value.ProhibitedTopics}
            }
        };
    }

    private static GrpcFetchResponse ToOk(
        IEnumerable<GrpcCurrentPartitionOwner> partitionOwners,
        ModelFetchResponse.Ok value)
    {
        return new GrpcFetchResponse
        {
            CurrentPartitionOwners = {partitionOwners},
            Ok = new GrpcFetchResponse.Types.FetchResponseOneOfOk
            {
                Messages = { value.Messages.Select(message => message.ToGrpc()) }
            }
        };
    }
}