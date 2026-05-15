namespace Zamza.Server.Models.ConsumerApi.Monitoring;

public sealed record ConsumerHeartbeat(
    string ConsumerId,
    string ConsumerGroup,
    DateTimeOffset TimestampUtc);