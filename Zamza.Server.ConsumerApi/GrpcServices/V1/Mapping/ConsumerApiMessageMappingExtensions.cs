using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using ModelConsumerApiMessage = Zamza.Server.Models.ConsumerApi.ConsumerApiMessage;
using GrpcConsumerApiMessage = Zamza.ConsumerApi.V1.ZamzaMessage;

namespace Zamza.Server.ConsumerApi.GrpcServices.V1.Mapping;

internal static class ConsumerApiMessageMappingExtensions
{
    public static GrpcConsumerApiMessage ToGrpc(this ModelConsumerApiMessage message)
    {
        return new GrpcConsumerApiMessage
        {
            ConsumerGroup = message.ConsumerGroup,
            Topic = message.Topic,
            Partition = message.Partition,
            Offset = message.Offset,
            Headers =
            {
                message.Headers.ToDictionary(
                    header => header.Key,
                    header => ByteString.CopyFrom(header.Value))
            },
            Key = message.Key is not null 
                ? ByteString.CopyFrom(message.Key) 
                : null,
            Value = message.Value is not null
                ? ByteString.CopyFrom(message.Value)
                : null,
            Timestamp = message.Timestamp.ToTimestamp(),
            MaxRetries = message.MaxRetries,
            MinRetriesGapMs = message.MinRetriesGapMs,
            ProcessingPeriodMs = message.ProcessingPeriodMs,
            RetriesCount = message.RetriesCount
        };
    }
}