namespace Zamza.Server.DataAccess.Utils.DateTimeProvider;

internal interface IDateTimeProvider
{
    DateTimeOffset UtcNow { get; }
}