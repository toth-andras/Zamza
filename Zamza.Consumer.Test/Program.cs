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
    topics: ["test_topic"]);

consumerBuilder.ConfigureMessageProcessing(
    new MessageProcessingOptions<string, string>
    {
        MinRetriesGap = TimeSpan.Zero,
        MaxRetriesCount = 3
    });

var consumer = consumerBuilder.Build(diContainer.BuildServiceProvider());
await consumer.ExecuteAsync(CancellationToken.None);