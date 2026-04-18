// See https://aka.ms/new-console-template for more information

using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Zamza.Consumer;
using Zamza.Consumer.Options;
using Zamza.Consumer.Test;

var diContainer = new ServiceCollection();
diContainer.AddLogging(x => x.AddSimpleConsole());
diContainer.AddTransient<MessageProcessor>();

var kafkaConfig = new ConsumerConfig
{
    GroupId = "consumer_group_test",
    BootstrapServers = "localhost:9092",
    EnableAutoCommit = false
};

var consumerBuilder = new ZamzaConsumerBuilder<string, string, MessageProcessor>(
    kafkaConfig,
    zamzaServerUri: new Uri("http://localhost:5000"),
    topics: ["topic1"]);

consumerBuilder.ConfigureMessageProcessing(
    new MessageProcessingOptions<string, string>
    {
        MinRetriesGap = TimeSpan.FromSeconds(1),
        MaxRetriesCount = 4,
        RetryGapEvaluator = message => TimeSpan.FromSeconds(2 * (message.RetriesCount + 1))
    });

var consumer = consumerBuilder.Build(diContainer.BuildServiceProvider());
await consumer.ExecuteAsync(CancellationToken.None);