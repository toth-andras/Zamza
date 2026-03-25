using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Zamza.Server.Application.ConsumerApi.Fetch.Models;
using GrpcRequest = Zamza.ConsumerApi.V1.FetchRequest;
using ModelRequest = Zamza.Server.Application.ConsumerApi.Fetch.Models.FetchRequest;
using GrpcFetchedPartition = Zamza.ConsumerApi.V1.FetchRequest.Types.FetchedPartition;
using ModelFetchedPartition = Zamza.Server.Models.ConsumerApi.Fetch.FetchedPartition;
using GrpcFetchedMessage = Zamza.ConsumerApi.V1.FetchResponse.Types.FetchResultOk.Types.FetchedMessage;
using ModelFetchedMessage = Zamza.Server.Models.ConsumerApi.Fetch.FetchedMessage;
using GrpcResponse = Zamza.ConsumerApi.V1.FetchResponse;
using ModelResponse = Zamza.Server.Application.ConsumerApi.Fetch.Models.FetchResponse;

namespace Zamza.Server.ConsumerApi.GrpcServices.V1.Mapping;

internal static class FetchMappingExtensions
{
    public static ModelRequest ToModel(this GrpcRequest grpcRequest)
    {
        return new ModelRequest(
            grpcRequest.ConsumerId,
            grpcRequest.ConsumerGroup,
            grpcRequest.Partitions
                .Select(partition => partition.ToModel())
                .ToList(),
            grpcRequest.Limit);
    }

    public static GrpcResponse ToGrpc(this ModelResponse response)
    {
        return response.Result switch
        {
            FetchResult.Ok => AsOkResponse(response),
            FetchResult.ObsoleteOwnership => AsOwnershipObsoleteResponse(response),

            _ => throw new ArgumentOutOfRangeException(
                paramName: "Fetch response result",
                actualValue: response.Result,
                message: "Not supported result")
        };
    }

    private static ModelFetchedPartition ToModel(this GrpcFetchedPartition partition)
    {
        return new ModelFetchedPartition(
            partition.Topic,
            partition.Partition,
            partition.OwnershipEpoch,
            partition.KafkaOffset);
    }

    private static GrpcResponse AsOkResponse(ModelResponse response)
    {
        return new GrpcResponse
        {
            CurrentOwnershipsForConsumerGroup = response.ConsumerGroupPartitionOwnerships.ToGrpc(),
            Ok = new GrpcResponse.Types.FetchResultOk
            {
                Messages =
                {
                    response.FetchedMessages!.Select(message => message.ToGrpc())
                }
            }
        };
    }

    private static GrpcResponse AsOwnershipObsoleteResponse(ModelResponse response)
    {
        return new GrpcResponse
        {
            CurrentOwnershipsForConsumerGroup = response.ConsumerGroupPartitionOwnerships.ToGrpc(),
            ObsoleteOwnership = new GrpcResponse.Types.FetchResultObsoleteOwnership()
        };
    }
    
    private static GrpcFetchedMessage ToGrpc(this ModelFetchedMessage fetchedMessage)
    {
        return new GrpcFetchedMessage
        {
            Topic = fetchedMessage.Topic,
            Partition = fetchedMessage.Partition,
            Offset = fetchedMessage.Offset,
            Headers =
            {
                fetchedMessage.Headers.ToDictionary(
                    header => header.Key,
                    header => ByteString.CopyFrom(header.Value))
            },
            Key = fetchedMessage.Key is not null
                ? ByteString.CopyFrom(fetchedMessage.Key)
                : null,
            Value = fetchedMessage.Value is not null
                ? ByteString.CopyFrom(fetchedMessage.Value)
                : null,
            Timestamp = fetchedMessage.Timestamp.ToTimestamp(),
            MaxRetriesCount = fetchedMessage.MaxRetriesCount,
            RetriesCount = fetchedMessage.RetriesCount,
            ProcessingDeadlineUtc = fetchedMessage.ProcessingDeadlineUtc?.ToTimestamp()
        };
    }
}