using FluentAssertions;
using Zamza.Server.Models.ConsumerApi.Commit;
using Zamza.Server.Models.Exceptions;

namespace Zamza.Server.UnitTests.Models;

public sealed class RetryableMessageTests
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

        // Act
        var act = () => new RetryableMessage(
            topic!,
            0,
            1,
            new Dictionary<string, byte[]>(),
            null,
            null,
            timestamp,
            3,
            0,
            null,
            1000);

        // Assert
        act.Should()
            .Throw<BadRequestException>()
            .WithMessage("Retryable message Topic cannot be empty");
    }

    [Fact]
    public void Constructor_ShouldThrowBadRequestException_WhenHeadersAreNull()
    {
        // Arrange
        var timestamp = new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.Zero);

        // Act
        var act = () => new RetryableMessage(
            "topic",
            0,
            1,
            null!,
            null,
            null,
            timestamp,
            3,
            0,
            null,
            1000);

        // Assert
        act.Should()
            .Throw<BadRequestException>()
            .WithMessage("Retryable message Headers cannot be null");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(int.MinValue)]
    public void Constructor_ShouldThrowBadRequestException_WhenMaxRetriesCountIsNotPositive(int maxRetriesCount)
    {
        // Arrange
        var timestamp = new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.Zero);

        // Act
        Action act = () => new RetryableMessage(
            "topic",
            0,
            1,
            new Dictionary<string, byte[]>(),
            null,
            null,
            timestamp,
            maxRetriesCount,
            0,
            null,
            1000);

        // Assert
        act.Should()
            .Throw<BadRequestException>()
            .WithMessage("Retryable message MaxRetriesCount must be positive");
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(int.MinValue)]
    public void Constructor_ShouldThrowBadRequestException_WhenRetriesCountIsNegative(int retriesCount)
    {
        // Arrange
        var timestamp = new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.Zero);

        // Act
        var act = () => new RetryableMessage(
            "topic",
            0,
            1,
            new Dictionary<string, byte[]>(),
            null,
            null,
            timestamp,
            3,
            retriesCount,
            null,
            1000);

        // Assert
        act.Should()
            .Throw<BadRequestException>()
            .WithMessage("Retryable message RetriesCount cannot be negative");
    }

    [Fact]
    public void Constructor_ShouldThrowBadRequestException_WhenRetriesCountExceedsMaxRetriesCount()
    {
        // Arrange
        var timestamp = new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.Zero);

        // Act
        var act = () => new RetryableMessage(
            "topic",
            0,
            1,
            new Dictionary<string, byte[]>(),
            null,
            null,
            timestamp,
            3,
            4,
            null,
            1000);

        // Assert
        act.Should()
            .Throw<BadRequestException>()
            .WithMessage("The message must be failed, not retryable, as the maximum number of retries exceeded.");
    }

    [Fact]
    public void Constructor_ShouldThrowBadRequestException_WhenProcessingDeadlineUtcIsNotUtc()
    {
        // Arrange
        var timestamp = new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.Zero);
        var processingDeadlineUtc = new DateTimeOffset(2026, 1, 1, 11, 0, 0, TimeSpan.FromHours(1));

        // Act
        var act = () => new RetryableMessage(
            "topic",
            0,
            1,
            new Dictionary<string, byte[]>(),
            null,
            null,
            timestamp,
            3,
            0,
            processingDeadlineUtc,
            1000);

        // Assert
        act.Should()
            .Throw<BadRequestException>()
            .WithMessage("Retryable message ProcessingDeadlineUtc must be provided in UTC");
    }

    [Fact]
    public void Constructor_ShouldThrowBadRequestException_WhenNextRetryAfterMsIsNegative()
    {
        // Arrange
        var timestamp = new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.Zero);

        // Act
        var act = () => new RetryableMessage(
            "topic",
            0,
            1,
            new Dictionary<string, byte[]>(),
            null,
            null,
            timestamp,
            3,
            0,
            null,
            -1);

        // Assert
        act.Should()
            .Throw<BadRequestException>()
            .WithMessage("Retryable message NextRetryAfterMs cannot be negative");
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
        const int maxRetriesCount = 5;
        const int retriesCount = 2;
        var processingDeadlineUtc = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);
        const long nextRetryAfterMs = 5000;

        // Act
        var retryableMessage = new RetryableMessage(
            topic,
            partition,
            offset,
            headers,
            key,
            value,
            timestamp,
            maxRetriesCount,
            retriesCount,
            processingDeadlineUtc,
            nextRetryAfterMs);

        // Assert
        retryableMessage.Topic.Should().Be(topic);
        retryableMessage.Partition.Should().Be(partition);
        retryableMessage.Offset.Should().Be(offset);
        retryableMessage.Headers.Should().BeSameAs(headers);
        retryableMessage.Key.Should().BeSameAs(key);
        retryableMessage.Value.Should().BeSameAs(value);
        retryableMessage.Timestamp.Should().Be(timestamp);
        retryableMessage.MaxRetriesCount.Should().Be(maxRetriesCount);
        retryableMessage.RetriesCount.Should().Be(retriesCount);
        retryableMessage.ProcessingDeadlineUtc.Should().Be(processingDeadlineUtc);
        retryableMessage.NextRetryAfterMs.Should().Be(nextRetryAfterMs);
    }
}