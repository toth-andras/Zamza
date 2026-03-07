namespace Zamza.Server.Application.ConsumerApi.Ping.Models;

public sealed record PingRequest(
    string ConsumerId,
    string ConsumerGroup);