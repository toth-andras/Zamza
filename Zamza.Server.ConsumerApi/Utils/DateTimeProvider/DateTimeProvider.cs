namespace Zamza.Server.ConsumerApi.Utils.DateTimeProvider;

internal sealed class DateTimeProvider : IDateTimeProvider
{
    public DateTimeOffset GetUtcNow() =>  DateTimeOffset.UtcNow;
}