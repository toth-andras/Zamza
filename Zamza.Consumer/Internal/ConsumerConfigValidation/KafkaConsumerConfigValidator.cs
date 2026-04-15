using Confluent.Kafka;

namespace Zamza.Consumer.Internal.ConsumerConfigValidation;

internal static class KafkaConsumerConfigValidator
{
    public static void Validate(ConsumerConfig config)
    {
        if (config.EnableAutoCommit is true)
        {
            throw new ArgumentException("enable.auto.commit must be false");
        }
    }
}