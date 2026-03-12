namespace Zamza.Server.Models.ConsumerApi;

public sealed record FailedMessage(
    ConsumerApiMessage Message,
    long NextRetryAfterMs);