namespace Zamza.Server.ConsumerApi.Utils.DateTimeProvider;

public interface IDateTimeProvider
{
    DateTimeOffset GetUtcNow();
}