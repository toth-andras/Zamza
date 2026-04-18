namespace Zamza.Server.Models.ConsumerApi.Monitoring;

public sealed record class ConsumerHeartbeat(
    string ConsumerId,
    string ConsumerGroup,
    DateTimeOffset TimestampUtc);