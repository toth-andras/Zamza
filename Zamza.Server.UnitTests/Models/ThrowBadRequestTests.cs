using FluentAssertions;
using Zamza.Server.Models.Exceptions;
using Zamza.Server.Models.Validators;

namespace Zamza.Server.UnitTests.Models;

public sealed class ThrowBadRequestTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\t")]
    public void IfEmpty_ShouldThrowBadRequestException_WhenStringIsNullOrWhiteSpace(string? str)
    {
        // Arrange
        const string paramName = "param";

        // Act
        Action act = () => ThrowBadRequest.IfEmpty(str, paramName);

        // Assert
        act.Should()
            .Throw<BadRequestException>()
            .WithMessage($"{paramName} cannot be empty");
    }

    [Fact]
    public void IfEmpty_ShouldNotThrow_WhenStringIsNotEmpty()
    {
        // Arrange
        const string str = "value";
        const string paramName = "name";

        // Act
        var act = () => ThrowBadRequest.IfEmpty(str, paramName);

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void IfNotUtc_ShouldThrowBadRequestException_WhenOffsetIsNotZero()
    {
        // Arrange
        var dateTimeOffset = new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.FromHours(1));
        const string paramName = "param";

        // Act
        var act = () => ThrowBadRequest.IfNotUtc(dateTimeOffset, paramName);

        // Assert
        act.Should()
            .Throw<BadRequestException>()
            .WithMessage($"{paramName} must be provided in UTC");
    }

    [Fact]
    public void IfNotUtc_ShouldNotThrow_WhenOffsetIsZero()
    {
        // Arrange
        var dateTimeOffset = new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.Zero);
        const string paramName = "param";

        // Act
        var act = () => ThrowBadRequest.IfNotUtc(dateTimeOffset, paramName);

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void IfNull_ShouldThrowBadRequestException_WhenObjectIsNull()
    {
        // Arrange
        object? obj = null;
        const string paramName = "param";

        // Act
        var act = () => ThrowBadRequest.IfNull(obj, paramName);

        // Assert
        act.Should()
            .Throw<BadRequestException>()
            .WithMessage($"{paramName} cannot be null");
    }

    [Fact]
    public void IfNull_ShouldNotThrow_WhenObjectIsNotNull()
    {
        // Arrange
        var obj = new object();
        const string paramName = "param";

        // Act
        var act = () => ThrowBadRequest.IfNull(obj, paramName);

        // Assert
        act.Should().NotThrow();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void IfNotPositive_ShouldThrowBadRequestException_WhenNumberIsZeroOrNegative(int number)
    {
        // Arrange
        const string paramName = "param";

        // Act
        Action act = () => ThrowBadRequest.IfNotPositive(number, paramName);

        // Assert
        act.Should()
            .Throw<BadRequestException>()
            .WithMessage($"{paramName} must be positive");
    }

    [Fact]
    public void IfNotPositive_ShouldNotThrow_WhenNumberIsPositive()
    {
        // Arrange
        const int number = 1;
        const string paramName = "param";

        // Act
        var act = () => ThrowBadRequest.IfNotPositive(number, paramName);

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void IfNegative_ShouldThrowBadRequestException_WhenNumberIsNegative()
    {
        // Arrange
        const long number = -1;
        const string paramName = "param";

        // Act
        var act = () => ThrowBadRequest.IfNegative(number, paramName);

        // Assert
        act.Should()
            .Throw<BadRequestException>()
            .WithMessage($"{paramName} cannot be negative");
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(1L)]
    public void IfNegative_ShouldNotThrow_WhenNumberIsZeroOrPositive(long number)
    {
        // Arrange
        const string paramName = "param";

        // Act
        var act = () => ThrowBadRequest.IfNegative(number, paramName);

        // Assert
        act.Should().NotThrow();
    }
}