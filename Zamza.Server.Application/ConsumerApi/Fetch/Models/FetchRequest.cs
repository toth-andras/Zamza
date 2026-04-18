using Zamza.Server.Models.ConsumerApi.Fetch;
using Zamza.Server.Models.Validators;

namespace Zamza.Server.Application.ConsumerApi.Fetch.Models;

public sealed record FetchRequest
{
    public string ConsumerId { get; }
    public string ConsumerGroup { get; }
    public IReadOnlyCollection<FetchedPartition> Partitions { get; }
    public int Limit { get; }
    public DateTimeOffset TimestampUtc { get; }

    public FetchRequest(
        string consumerId,
        string consumerGroup,
        IReadOnlyCollection<FetchedPartition> partitions,
        int limit,
        DateTimeOffset timestampUtc)
    {
        ThrowBadRequest.IfEmpty(consumerId, "Fetch request ConsumerId");
        ThrowBadRequest.IfEmpty(consumerGroup, "Fetch request ConsumerGroup");
        ThrowBadRequest.IfNull(partitions, "Fetch request Partitions");
        ThrowBadRequest.IfNegative(limit, "Fetch request Limit");
        ThrowBadRequest.IfNotUtc(timestampUtc, "Fetch request timestamp");
        
        ConsumerId = consumerId;
        ConsumerGroup = consumerGroup;
        Partitions = partitions;
        Limit = limit;
        TimestampUtc = timestampUtc;
    }
}