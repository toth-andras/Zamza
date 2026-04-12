namespace Zamza.Consumer.Internal.Utils.DateTimeProvider;

internal sealed class DateTimeProvider : IDateTimeProvider
{
    public DateTime UtcNow => DateTime.UtcNow;
}