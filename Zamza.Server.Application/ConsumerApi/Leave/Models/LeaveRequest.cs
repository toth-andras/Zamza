using Zamza.Server.Models.Validators;

namespace Zamza.Server.Application.ConsumerApi.Leave.Models;

public sealed record LeaveRequest
{
    public string ConsumerId { get; }
    public string ConsumerGroup { get; }
    public DateTimeOffset TimestampUtc { get; }

    public LeaveRequest(
        string consumerId,
        string consumerGroup, 
        DateTimeOffset timestampUtc)
    {
        ThrowBadRequest.IfEmpty(consumerId, "Leave request ConsumerId");
        ThrowBadRequest.IfEmpty(consumerGroup, "Leave request ConsumerGroup");
        ThrowBadRequest.IfNotUtc(timestampUtc, "Leave request TimestampUtc");
        
        ConsumerId = consumerId;
        ConsumerGroup = consumerGroup;
        TimestampUtc = timestampUtc;
    }
}