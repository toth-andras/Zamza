namespace Zamza.Consumer.Internal.Configs.Specific;

internal sealed record PingConfig
{
    public TimeSpan PingInterval { get; }
    public TimeSpan MaxOfflineTime { get; }

    public PingConfig(TimeSpan pingInterval, TimeSpan maxOfflineTime)
    {
        ValidatePingInterval(pingInterval);
        ValidateMaxOfflineTime(maxOfflineTime);
        
        PingInterval = pingInterval;
        MaxOfflineTime = maxOfflineTime;
    }
    
    public static PingConfig Default => new(
        pingInterval: TimeSpan.FromSeconds(15), 
        maxOfflineTime: TimeSpan.FromMinutes(5));

    private static void ValidatePingInterval(TimeSpan timeSpan)
    {
        if (timeSpan >= TimeSpan.Zero)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(
            message: "Ping interval must be a non-negative TimeSpan",
            innerException: null);
    }

    private static void ValidateMaxOfflineTime(TimeSpan timeSpan)
    {
        if (timeSpan >= TimeSpan.Zero)
        {
            return;
        }
        
        throw new ArgumentOutOfRangeException(
            message: "Max offline time must be a non-negative TimeSpan",
            innerException: null);
    }
}