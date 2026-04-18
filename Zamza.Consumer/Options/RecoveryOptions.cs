namespace Zamza.Consumer.Options;

public sealed class RecoveryOptions
{
    /// <summary>
    /// The interval between ping calls to Zamza.Server.
    /// </summary>
    public TimeSpan PingInterval { get; init; } = TimeSpan.FromSeconds(10);
        
    /// <summary>
    /// The maximum time Zamza.Server is allowed not to respond before being marked as offline.
    /// </summary>
    /// <remarks>
    /// If Zamza.Server is offline, the consumer will be stopped.
    /// </remarks>
    public TimeSpan MaxOfflineTime { get; init; } = TimeSpan.FromMinutes(5);
}