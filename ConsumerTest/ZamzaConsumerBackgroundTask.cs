using Confluent.Kafka;
using Zamza.Consumer;

namespace ConsumerTest;

public sealed class ZamzaConsumerBackgroundTask : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;

    public ZamzaConsumerBackgroundTask(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var kafkaConfig = new ConsumerConfig
        {
            GroupId = "consumer_group_test",
            BootstrapServers = "localhost:9092",
            EnableAutoCommit = false
        };
        
        var zamzaConsumer = ZamzaConsumerBuilder<string, string, TestMessageProcessor>
            .Setup(kafkaConfig, new Uri("http://localhost:5000"), ["topic1"])
            .Build(_serviceProvider);

        try
        {
            await zamzaConsumer.ExecuteAsync(stoppingToken);
        }
        catch
        {
            // pass
        }
    }
}