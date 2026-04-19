using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Prometheus;
using Zamza.Server.DataAccess.Repositories.DLQRepository;
using Zamza.Server.DataAccess.Repositories.InstanceLeadershipRepository;
using Zamza.Server.DataAccess.Repositories.RetryQueueRepository;

namespace Zamza.Server.Application.Observability.BackgroundTasks.MessageQueueSize;

internal sealed class MessageQueuesSizeBackgroundTask : BackgroundTaskWithLeadership
{
    private readonly IServiceProvider _serviceProvider;

    private static readonly string ServerInstanceStr = ObservabilityContstants.ServiceInstanceId.ToString();
    
    private static readonly Gauge RetryQueueSizeGauge = Metrics.CreateGauge(
        "zamza_retry_queue_size",
        "The number of messages in retry queue",
        new GaugeConfiguration
        {
            LabelNames = ["server_instance"]
        });
    
    private static readonly Gauge DLQSizeGauge = Metrics.CreateGauge(
        "zamza_dlq_size",
        "The number of messages in DLQ",
        new GaugeConfiguration
        {
            LabelNames = ["server_instance"]
        });
    
    protected override string BackgroundTaskName => "MessageQueuesSize";
    protected override TimeSpan CycleTime => TimeSpan.FromSeconds(20);

    public MessageQueuesSizeBackgroundTask(
        IInstanceLeadershipRepository leadershipRepository,
        ILogger<BackgroundTaskWithLeadership> logger, 
        IServiceProvider serviceProvider) : base(leadershipRepository, logger)
    {
        _serviceProvider = serviceProvider;
    }
    
    protected override async Task ExecuteCycle(CancellationToken cancellationToken)
    {
        using var scope = _serviceProvider.CreateScope();
        var retryQueueRepository = scope.ServiceProvider.GetRequiredService<IRetryQueueRepository>();
        var dlqRepository = scope.ServiceProvider.GetRequiredService<IDLQRepository>();
        
        var retryQueueSize = await retryQueueRepository.GetRetryQueueSize(cancellationToken);
        var dlqSize = await dlqRepository.GetDLQSize(cancellationToken);
        
        RetryQueueSizeGauge
            .WithLabels([ServerInstanceStr])
            .Set(retryQueueSize);
        
        DLQSizeGauge
            .WithLabels([ServerInstanceStr])
            .Set(dlqSize);
    }
}