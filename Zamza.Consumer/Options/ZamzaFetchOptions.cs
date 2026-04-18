using Zamza.Consumer.Internal.Configs.Specific;

namespace Zamza.Consumer.Options;

public sealed class ZamzaFetchOptions
{
    /// <summary>
    /// When consuming messages, a fetch from Zamza.Server will be done after
    /// every KafkaCallsPerZamzaCall fetches from Kafka.
    /// </summary>
    public int KafkaConsumesPerZamzaFetch { get; init; } = ZamzaFetchConfig.Default.KafkaConsumesPerZamzaFetch;
    
    /// <summary>
    /// No more than this many messages will be fetched from Zamza.Server in one request.
    /// </summary>
    public int FetchLimit { get; init; } = ZamzaFetchConfig.Default.FetchLimit;
}