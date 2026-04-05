namespace Zamza.Server.Application.ConsumerApi.Leave.Models;

public sealed record LeaveResponse
{
    public static LeaveResponse Instance { get; } = new();
};