using FluentAssertions;
using Zamza.Server.Models.ConsumerApi.Commit;
using Zamza.Server.Models.Exceptions;

namespace Zamza.Server.UnitTests.Models;

public sealed class FailedMessageTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\t")]
    public void Constructor_ShouldThrowBadRequestException_WhenTopicIsNullOrWhiteSpace(string? topic)
    {
        // Arrange
        var timestamp = new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.Zero);
        var failedAtUtc = new DateTimeOffset(2026, 1, 1, 11, 0, 0, TimeSpan.Zero);

        // Act
        var act = () => new FailedMessage(
            topic!,
            0,
            1,
            new Dictionary<string, byte[]>(),
            null,
            null,
            timestamp,
            0,
            failedAtUtc);

        // Assert
        act.Should()
            .Throw<BadRequestException>()
            .WithMessage("Failed message Topic cannot be empty");
    }

    [Fact]
    public void Constructor_ShouldThrowBadRequestException_WhenHeadersAreNull()
    {
        // Arrange
        var timestamp = new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.Zero);
        var failedAtUtc = new DateTimeOffset(2026, 1, 1, 11, 0, 0, TimeSpan.Zero);

        // Act
        var act = () => new FailedMessage(
            "topic",
            0,
            1,
            null!,
            null,
            null,
            timestamp,
            0,
            failedAtUtc);

        // Assert
        act.Should()
            .Throw<BadRequestException>()
            .WithMessage("Failed message Headers cannot be null");
    }

    [Fact]
    public void Constructor_ShouldThrowBadRequestException_WhenRetriesCountIsNegative()
    {
        // Arrange
        var timestamp = new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.Zero);
        var failedAtUtc = new DateTimeOffset(2026, 1, 1, 11, 0, 0, TimeSpan.Zero);

        // Act
        var act = () => new FailedMessage(
            "topic",
            0,
            1,
            new Dictionary<string, byte[]>(),
            null,
            null,
            timestamp,
            -1,
            failedAtUtc);

        // Assert
        act.Should()
            .Throw<BadRequestException>()
            .WithMessage("Failed message RetriesCount cannot be negative");
    }

    [Fact]
    public void Constructor_ShouldThrowBadRequestException_WhenFailedAtUtcIsNotUtc()
    {
        // Arrange
        var timestamp = new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.Zero);
        var failedAtUtc = new DateTimeOffset(2026, 1, 1, 11, 0, 0, TimeSpan.FromHours(1));

        // Act
        var act = () => new FailedMessage(
            "topic",
            0,
            1,
            new Dictionary<string, byte[]>(),
            null,
            null,
            timestamp,
            0,
            failedAtUtc);

        // Assert
        act.Should()
            .Throw<BadRequestException>()
            .WithMessage("Failed message FailedAtUtc must be provided in UTC");
    }

    [Fact]
    public void Constructor_ShouldSetProperties_WhenArgumentsAreValid()
    {
        // Arrange
        const string topic = "topic";
        const int partition = 1;
        const long offset = 10;
        var headers = new Dictionary<string, byte[]>
        {
            ["header-1"] = "header"u8.ToArray()
        };
        var key = "key"u8.ToArray();
        var value = "value"u8.ToArray();
        var timestamp = new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.FromHours(2));
        const int retriesCount = 2;
        var failedAtUtc = new DateTimeOffset(2026, 1, 1, 11, 0, 0, TimeSpan.Zero);

        // Act
        var failedMessage = new FailedMessage(
            topic,
            partition,
            offset,
            headers,
            key,
            value,
            timestamp,
            retriesCount,
            failedAtUtc);

        // Assert
        failedMessage.Topic.Should().Be(topic);
        failedMessage.Partition.Should().Be(partition);
        failedMessage.Offset.Should().Be(offset);
        failedMessage.Headers.Should().BeSameAs(headers);
        failedMessage.Key.Should().BeSameAs(key);
        failedMessage.Value.Should().BeSameAs(value);
        failedMessage.Timestamp.Should().Be(timestamp);
        failedMessage.RetriesCount.Should().Be(retriesCount);
        failedMessage.FailedAtUtc.Should().Be(failedAtUtc);
    }
}