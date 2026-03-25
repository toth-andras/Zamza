using Zamza.Server.Models.Exceptions;

namespace Zamza.Server.Models.Validators;

public static class Throw
{
    public static void IfEmpty(string? str, string paramName)
    {
        if (string.IsNullOrWhiteSpace(str))
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

    public static void IfNull(object? obj, string paramName)
    {
        if (obj is null)
        {
            throw new BadRequestException($"{paramName} cannot be null");
        }
    }

    public static void IfNotPositive(int number, string paramName)
    {
        if (number <= 0)
        {
            throw new BadRequestException($"{paramName} must be positive");
        }
    }

    public static void IfNegative(int number, string paramName)
    {
        if (number < 0)
        {
            throw new BadRequestException($"{paramName} cannot be negative");
        }
    }
}