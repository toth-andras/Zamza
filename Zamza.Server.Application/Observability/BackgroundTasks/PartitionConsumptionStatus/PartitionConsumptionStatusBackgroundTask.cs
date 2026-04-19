using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Prometheus;
using Zamza.Server.DataAccess.Repositories.ConsumerHeartbeatRepository;
using Zamza.Server.DataAccess.Repositories.InstanceLeadershipRepository;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository;

namespace Zamza.Server.Application.Observability.BackgroundTasks.PartitionConsumptionStatus;

internal sealed class PartitionConsumptionStatusBackgroundTask : BackgroundTaskWithLeadership
{
    private readonly IServiceProvider _serviceProvider;

    private static readonly Gauge PartitionStatusGauge = Metrics.CreateGauge(
        "zamza_partition_consumption_status",
        "The status of consumption for partitions",
        new GaugeConfiguration
        {
            LabelNames = ["server_instance", "consumer_group", "topic", "partition"]
        });
    
    protected override string BackgroundTaskName => "PartitionConsumptionStatus";
    protected override TimeSpan CycleTime => TimeSpan.FromSeconds(30);

    public PartitionConsumptionStatusBackgroundTask(
        IInstanceLeadershipRepository leadershipRepository,
        ILogger<BackgroundTaskWithLeadership> logger,
        IServiceProvider serviceProvider) : base(leadershipRepository, logger)
    {
        _serviceProvider = serviceProvider;
    }
    
    protected override async Task ExecuteCycle(CancellationToken cancellationToken)
    {
        Console.WriteLine("metrics collecting");
        using var scope = _serviceProvider.CreateScope();
        
        var consumerHeartbeatRepository = scope.ServiceProvider.GetRequiredService<IConsumerHeartbeatRepository>();

        var now = DateTimeOffset.UtcNow;
        var offlineConsumers = await consumerHeartbeatRepository.GetOfflineConsumers(
            now.Subtract(TimeSpan.FromMinutes(5)),
            cancellationToken);
        
        var offlineConsumersSet = offlineConsumers
            .Select(consumer => (consumer.ConsumerGroup, consumer.ConsumerId))
            .ToHashSet();
        
        var partitionOwnershipRepository = scope.ServiceProvider.GetRequiredService<IPartitionOwnershipRepository>();
        var partitionsEnumerable = partitionOwnershipRepository.List(cancellationToken);

        const int consumptionStatusOk = 1;
        const int consumptionStatusOfflineConsumer = 2;
        const int consumptionStatusUnowned = 3;
        await foreach (var partitionOwnership in partitionsEnumerable)
        {
            var labelledMetric = PartitionStatusGauge
                .WithLabels([
                    ServerInstanceId.ToString(),
                    partitionOwnership.ConsumerGroup,
                    partitionOwnership.Topic,
                    partitionOwnership.Partition.ToString()
                ]);
            
            if (partitionOwnership.OwnerConsumerId is null)
            {
                if (partitionOwnership.TimestampUtc < now.Subtract(TimeSpan.FromMinutes(5)))
                {
                    labelledMetric.Set(consumptionStatusUnowned);
                }
                continue;
            }

            if (offlineConsumersSet.Contains((partitionOwnership.ConsumerGroup, partitionOwnership.OwnerConsumerId!)))
            {
                labelledMetric.Set(consumptionStatusOfflineConsumer);
                continue;
            }
            
            labelledMetric.Set(consumptionStatusOk);
        }
    }
}