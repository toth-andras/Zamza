using Zamza.Server.Models.ConsumerApi.Monitoring;

namespace Zamza.Server.DataAccess.Repositories.ConsumerHeartbeatRepository;

public interface IConsumerHeartbeatRepository
{
    Task SaveHeartbeat(
        ConsumerHeartbeat heartbeat,
        CancellationToken cancellationToken);
    
    Task DeleteConsumer(
        string consumerId,
        string consumerGroup,
        CancellationToken cancellationToken);
}