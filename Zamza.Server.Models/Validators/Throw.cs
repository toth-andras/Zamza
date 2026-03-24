using Zamza.Server.Models.Exceptions;

namespace Zamza.Server.Models.Validators;

public static class Throw
{
    public static void IfEmpty(string? str, string paramName)
    {
        if (string.IsNullOrEmpty(str))
        {
            throw new BadRequestException($"{paramName} cannot be empty");
        }
    }

    public static void IfNotUtc(DateTimeOffset dateTimeOffset, string paramName)
    {
        if (dateTimeOffset.Offset != TimeSpan.Zero)
        {
            throw new BadRequestException($"{paramName} must be provided in UTC");
        }
    }
}