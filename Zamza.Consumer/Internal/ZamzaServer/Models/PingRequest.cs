namespace Zamza.Consumer.Internal.ZamzaServer.Models;

public sealed record PingRequest(
    string ConsumerId,
    string ConsumerGroup);