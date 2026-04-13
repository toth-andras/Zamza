namespace Zamza.Consumer.Internal.ZamzaServer.Models;

internal sealed record LeaveRequest(
    string ConsumerId,
    string ConsumerGroup);