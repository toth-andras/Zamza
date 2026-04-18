namespace Zamza.Server.DataAccess.Repositories.InstanceLeadershipRepository;

public interface IInstanceLeadershipRepository
{
    public Task<bool> TryBecomeLeader(
        string key,
        Guid instanceId,
        TimeSpan leadershipPeriod,
        CancellationToken cancellationToken);
}