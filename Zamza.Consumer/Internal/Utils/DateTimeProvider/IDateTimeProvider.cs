namespace Zamza.Consumer.Internal.Utils.DateTimeProvider;

internal interface IDateTimeProvider
{
    DateTime UtcNow { get; }
}