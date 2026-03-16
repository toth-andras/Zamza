using System.Collections.ObjectModel;
using Confluent.Kafka;
using Grpc.Net.Client;
using Zamza.Consumer.Models;
using Zamza.Consumer.Models.ConsumerMetadata;
using Zamza.Consumer.ServerFacade.RequestsResponses;
using Zamza.ConsumerApi.V1;
using CommitResponse = Zamza.Consumer.ServerFacade.RequestsResponses.CommitResponse;

namespace Zamza.Consumer.ServerFacade;

internal sealed class ZamzaServerFacade<TKey, TValue>
{
    private readonly TimeSpan _hardTimeout = TimeSpan.FromMinutes(10);
    private readonly ConsumerApiV1.ConsumerApiV1Client _grpcClient;

    public ZamzaServerFacade(Uri zamzaServerUri)
    {
        var channel = GrpcChannel.ForAddress(zamzaServerUri);
        _grpcClient = new ConsumerApiV1.ConsumerApiV1Client(channel);
    }

    public async Task<ClaimOwnershipResponse> ClaimOwnership(
        IReadOnlyCollection<TopicPartition> ownedPartitionsFromKafka,
        ConsumerMetadata<TKey, TValue> metadata,
        CancellationToken cancellationToken)
    {
        var claimsList = new List<ClaimPartitionOwnershipRequest.Types.PartitionOwnershipClaim>(metadata.OwnedPartitions.Count);
        const int initialOwnershipEpoch = 0;
        foreach (var claim in ownedPartitionsFromKafka)
        {
            var claimedOwnershipEpoch = metadata.PartitionOwnershipsOfConsumerGroup.TryGetValue((claim.Topic, claim.Partition.Value), out var ownership)
                ? ownership.OwnershipEpoch
                : initialOwnershipEpoch;
            
            claimsList.Add(new ClaimPartitionOwnershipRequest.Types.PartitionOwnershipClaim
            {
                Topic = claim.Topic,
                Partition = claim.Partition.Value,
                KnownOwnershipEpoch = claimedOwnershipEpoch
            });
        }
        
        var request = new ClaimPartitionOwnershipRequest
        {
            ConsumerId = metadata.ConsumerId,
            ConsumerGroup = metadata.ConsumerGroup,
            Claims = {claimsList}
        };

        const string authorizationHeaderName = "Authorization";
        var headers = new Grpc.Core.Metadata
        {
            {authorizationHeaderName, $"Bearer {metadata.BearerToken}"}
        };

        try
        {
            var grpcResponse = await _grpcClient.ClaimPartitionOwnershipAsync(
                request,
                headers: headers,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            var partitionOwnerships = grpcResponse.CurrentPartitionOwners
                .Select(ownership => new PartitionOwnership(
                    ownership.Topic,
                    ownership.Partition,
                    ownership.OwnershipEpoch))
                .ToList();

            return grpcResponse.BodyCase switch
            {
                ClaimPartitionOwnershipResponse.BodyOneofCase.PartitionOwnershipObsolete => new ClaimOwnershipResponse(
                    ClaimOwnershipResponse.OwnershipObsolete,
                    partitionOwnerships,
                    null),

                ClaimPartitionOwnershipResponse.BodyOneofCase.PermissionDenied => new ClaimOwnershipResponse(
                    ClaimOwnershipResponse.PermissionDenied,
                    partitionOwnerships,
                    grpcResponse.PermissionDenied.ProhibitedTopics),

                ClaimPartitionOwnershipResponse.BodyOneofCase.Ok => new ClaimOwnershipResponse(
                    ClaimOwnershipResponse.Ok,
                    partitionOwnerships,
                    []),

                _ => throw new Exception()
            };
        }
        catch (Exception)
        {
            throw;
        }
    }

    public async Task<FetchResponse<TKey, TValue>> Fetch(
        ConsumerMetadata<TKey, TValue> metadata,
        CancellationToken cancellationToken)
    {
        var request = new FetchRequest
        {
            ConsumerId = metadata.ConsumerId,
            ConsumerGroup = metadata.ConsumerGroup,
            Partitions =
            {
                metadata.OwnedPartitions.Select(topicPartition => new FetchRequest.Types.PartitionFetch
                {
                    Topic = topicPartition.Topic,
                    Partition = topicPartition.Partition,
                    KafkaOffset = metadata.CommitedKafkaOffsets.GetValueOrDefault((topicPartition.Topic, topicPartition.Partition.Value), 0),
                    OwnershipEpoch = metadata
                        .PartitionOwnershipsOfConsumerGroup[(topicPartition.Topic, topicPartition.Partition)]
                        .OwnershipEpoch
                })
            },
            Limit = metadata.FetchLimit
        };

        var headers = CreateHeaders(metadata);

        try
        {
            var grpcResponse = await _grpcClient.FetchAsync(
                request,
                headers: headers,
                deadline: DateTime.UtcNow.Add(_hardTimeout),
                cancellationToken: cancellationToken).ConfigureAwait(false);

            var partitionOwnerships = ToModelPartitionOwnershipList(grpcResponse.CurrentPartitionOwners);

            return grpcResponse.BodyCase switch
            {
                FetchResponse.BodyOneofCase.PartitionOwnershipObsolete => new FetchResponse<TKey, TValue>(
                    FetchResponse<TKey, TValue>.PartitionOwnershipObsolete,
                    partitionOwnerships,
                    Messages: [],
                    ProhibitedTopics: ReadOnlySet<string>.Empty),

                FetchResponse.BodyOneofCase.PermissionDenied => new FetchResponse<TKey, TValue>(
                    FetchResponse<TKey, TValue>.PermissionDenied,
                    partitionOwnerships,
                    Messages: [],
                    grpcResponse.PermissionDenied.ProhibitedTopics.ToHashSet()),

                FetchResponse.BodyOneofCase.Ok => new FetchResponse<TKey, TValue>(
                    FetchResponse<TKey, TValue>.Ok,
                    partitionOwnerships,
                    Messages: grpcResponse.Ok.Messages
                        .Select(message => ZamzaMessageFactoryForZamzaServer<TKey, TValue>.CreateModelMessage(message))
                        .ToList(),
                    ProhibitedTopics: ReadOnlySet<string>.Empty),
                
                _ => throw new ArgumentOutOfRangeException()
            };
        }
        catch (Exception)
        {
            throw;
        }
    }

    public async Task<CommitResponse> Commit(
        ConsumerMetadata<TKey, TValue> metadata,
        IReadOnlyCollection<ZamzaMessage<TKey, TValue>> processedMessages,
        IReadOnlyCollection<(ZamzaMessage<TKey, TValue> Message, TimeSpan NextRetryAfter)> failedMessages,
        IReadOnlyCollection<ZamzaMessage<TKey, TValue>> poisonedMessages,
        CancellationToken cancellationToken)
    {

        var commitRequest = new CommitRequest
        {
            ConsumerId = metadata.ConsumerId,
            ConsumerGroup = metadata.ConsumerGroup,
            OwnedPartitions = {ToGrpcPartitionOwnerList(metadata.PartitionOwnershipsOfConsumerGroup.Values.ToList())},
            ProcessedMessages =
            {
                processedMessages.Select(message => new CommitRequest.Types.ProcessedMessage
                {
                    Topic = message.Topic,
                    Partition = message.Partition,
                    Offset = message.Offset
                })
            },
            FailedMessages =
            {
                failedMessages.Select(message => new CommitRequest.Types.FailedMessage
                {
                    Message = ZamzaMessageFactoryForZamzaServer<TKey, TValue>.CreateGrpcMessage(message.Message,
                        metadata.ConsumerGroup),
                    NextRetryAfterMs = (long) message.NextRetryAfter.TotalMilliseconds
                })
            },
            PoisonedMessages =
            {
                poisonedMessages.Select(message =>
                    ZamzaMessageFactoryForZamzaServer<TKey, TValue>.CreateGrpcMessage(message, metadata.ConsumerGroup))
            }
        };
        
        var headers = CreateHeaders(metadata);

        try
        {
            var grpcResponse = await _grpcClient.CommitAsync(
                commitRequest,
                headers: headers,
                deadline: DateTime.UtcNow.Add(_hardTimeout),
                cancellationToken: cancellationToken
            ).ConfigureAwait(false);

            return new CommitResponse(
                ToModelPartitionOwnershipList(grpcResponse.CurrentPartitionOwners),
                grpcResponse.UnownedPartitionsMessages
                    .Select(message => (message.Topic, message.Partition, message.Offset)).ToHashSet(),
                grpcResponse.ProhibitedTopicsMessages
                    .Select(message => (message.Topic, message.Partition, message.Offset)).ToHashSet());
        }
        catch (Exception)
        {
            throw;
        }
    }

    private static Grpc.Core.Metadata CreateHeaders(ConsumerMetadata<TKey, TValue> metadata)
    {
        const string authorizationHeaderName = "Authorization";
        return new Grpc.Core.Metadata
        {
            {authorizationHeaderName, $"Bearer {metadata.BearerToken}"}
        };
    }

    private List<PartitionOwnership> ToModelPartitionOwnershipList(
        IReadOnlyCollection<CurrentPartitionOwner> partitionOwners)
    {
        var list = new List<PartitionOwnership>(partitionOwners.Count);
        foreach (var partitionOwner in partitionOwners)
        {
            list.Add(new PartitionOwnership(partitionOwner.Topic, partitionOwner.Partition, partitionOwner.OwnershipEpoch));
        }
        
        return list;
    }

    private List<ConsumerPartitionOwnershipEpoch> ToGrpcPartitionOwnerList(
        IReadOnlyCollection<PartitionOwnership> ownerships)
    {
        return ownerships
            .Select(ownership => new ConsumerPartitionOwnershipEpoch
            {
                Topic = ownership.Topic,
                Partition = ownership.Partition,
                OwnershipEpoch = ownership.OwnershipEpoch
            })
            .ToList();
    }
}