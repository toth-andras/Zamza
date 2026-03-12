namespace Zamza.Server.Application.ConsumerApi.Leave.Models;

public sealed record LeaveRequest(
    string ConsumerId,
    string ConsumerGroup);