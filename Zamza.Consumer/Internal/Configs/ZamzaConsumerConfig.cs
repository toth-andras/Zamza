using Zamza.Consumer.Internal.Configs.Specific;

namespace Zamza.Consumer.Internal.Configs;

internal sealed record ZamzaConsumerConfig<TKey, TValue>(
    MainInfoConfig MainInfo,
    MessageProcessorConfig<TKey, TValue> MessageProcessor,
    ZamzaFetchConfig ZamzaFetch,
    PingConfig Ping);