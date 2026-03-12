namespace Zamza.Server.DataAccess.Utils.DateTimeProvider;

internal sealed class DateTimeProvider : IDateTimeProvider
{
    public DateTimeOffset UtcNow => DateTimeOffset.UtcNow;
}