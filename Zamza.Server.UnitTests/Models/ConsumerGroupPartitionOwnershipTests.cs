using FluentAssertions;
using Zamza.Server.Models.ConsumerApi.Common;
using Zamza.Server.Models.Exceptions;

namespace Zamza.Server.UnitTests.Models;

public sealed class ConsumerGroupPartitionOwnershipTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\t")]
    public void Constructor_ShouldThrowBadRequestException_WhenConsumerGroupIsNullOrWhiteSpace(string? consumerGroup)
    {
        // Arrange
        var timestampUtc = new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.Zero);

        // Act
        var act = () => new ConsumerGroupPartitionOwnership(
            consumerGroup!,
            "topic",
            0,
            1,
            "consumer-id",
            timestampUtc);

        // Assert
        act.Should()
            .Throw<BadRequestException>()
            .WithMessage("Partition ownership consumer group cannot be empty");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\t")]
    public void Constructor_ShouldThrowBadRequestException_WhenTopicIsNullOrWhiteSpace(string? topic)
    {
        // Arrange
        var timestampUtc = new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.Zero);

        // Act
        var act = () => new ConsumerGroupPartitionOwnership(
            "consumer-group",
            topic!,
            0,
            1,
            "consumer-id",
            timestampUtc);

        // Assert
        act.Should()
            .Throw<BadRequestException>()
            .WithMessage("Partition ownership topic name cannot be empty");
    }

    [Fact]
    public void Constructor_ShouldThrowBadRequestException_WhenTimestampUtcIsNotUtc()
    {
        // Arrange
        var timestampUtc = new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.FromHours(1));

        // Act
        var act = () => new ConsumerGroupPartitionOwnership(
            "consumer-group",
            "topic",
            0,
            1,
            "consumer-id",
            timestampUtc);

        // Assert
        act.Should()
            .Throw<BadRequestException>()
            .WithMessage("Partition ownership timestamp must be provided in UTC");
    }

    [Fact]
    public void Constructor_ShouldSetProperties_WhenArgumentsAreValid()
    {
        // Arrange
        const string consumerGroup = "consumer-group";
        const string topic = "topic";
        const int partition = 1;
        const long ownerEpoch = 2;
        const string ownerConsumerId = "consumer-id";
        var timestampUtc = new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.Zero);

        // Act
        var ownership = new ConsumerGroupPartitionOwnership(
            consumerGroup,
            topic,
            partition,
            ownerEpoch,
            ownerConsumerId,
            timestampUtc);

        // Assert
        ownership.ConsumerGroup.Should().Be(consumerGroup);
        ownership.Topic.Should().Be(topic);
        ownership.Partition.Should().Be(partition);
        ownership.OwnerEpoch.Should().Be(ownerEpoch);
        ownership.OwnerConsumerId.Should().Be(ownerConsumerId);
        ownership.TimestampUtc.Should().Be(timestampUtc);
    }

    [Fact]
    public void Constructor_ShouldAllowNullOwnerConsumerId_WhenArgumentsAreValid()
    {
        // Arrange
        var timestampUtc = new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.Zero);

        // Act
        var ownership = new ConsumerGroupPartitionOwnership(
            "consumer-group",
            "topic",
            0,
            1,
            null,
            timestampUtc);

        // Assert
        ownership.OwnerConsumerId.Should().BeNull();
    }

    [Fact]
    public void CreateForNotRegisteredPartition_ShouldCreateOwnershipWithInitialOwnerEpoch()
    {
        // Arrange
        const string consumerGroup = "consumer-group";
        const string topic = "topic";
        const int partition = 1;
        const string consumerId = "consumer-id";
        var timestampUtc = new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.Zero);

        // Act
        var ownership = ConsumerGroupPartitionOwnership.CreateForNotRegisteredPartition(
            consumerGroup,
            topic,
            partition,
            consumerId,
            timestampUtc);

        // Assert
        ownership.ConsumerGroup.Should().Be(consumerGroup);
        ownership.Topic.Should().Be(topic);
        ownership.Partition.Should().Be(partition);
        ownership.OwnerEpoch.Should().Be(ConsumerGroupPartitionOwnership.InitialPartitionOwnerEpoch);
        ownership.OwnerConsumerId.Should().Be(consumerId);
        ownership.TimestampUtc.Should().Be(timestampUtc);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\t")]
    public void SetNewOwner_ShouldThrowBadRequestException_WhenNewOwnerConsumerIdIsNullOrWhiteSpace(string? newOwnerConsumerId)
    {
        // Arrange
        var ownership = new ConsumerGroupPartitionOwnership(
            "consumer-group",
            "topic",
            0,
            1,
            "consumer-id",
            new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.Zero));

        var timestampUtc = new DateTimeOffset(2026, 1, 1, 11, 0, 0, TimeSpan.Zero);

        // Act
        var act = () => ownership.SetNewOwner(newOwnerConsumerId!, 1, timestampUtc);

        // Assert
        act.Should()
            .Throw<BadRequestException>()
            .WithMessage("New partition owner ConsumerId cannot be empty");
    }

    [Fact]
    public void SetNewOwner_ShouldThrowBadRequestException_WhenTimestampUtcIsNotUtc()
    {
        // Arrange
        var ownership = new ConsumerGroupPartitionOwnership(
            "consumer-group",
            "topic",
            0,
            1,
            "consumer-id",
            new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.Zero));

        var timestampUtc = new DateTimeOffset(2026, 1, 1, 11, 0, 0, TimeSpan.FromHours(1));

        // Act
        var act = () => ownership.SetNewOwner("new-consumer-id", 1, timestampUtc);

        // Assert
        act.Should()
            .Throw<BadRequestException>()
            .WithMessage("Partition ownership claim timestamp must be provided in UTC");
    }

    [Fact]
    public void SetNewOwner_ShouldThrowBadRequestException_WhenPreviousOwnerEpochIsNotCorrect()
    {
        // Arrange
        var ownership = new ConsumerGroupPartitionOwnership(
            "consumer-group",
            "topic",
            0,
            1,
            "consumer-id",
            new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.Zero));

        var timestampUtc = new DateTimeOffset(2026, 1, 1, 11, 0, 0, TimeSpan.Zero);

        // Act
        var act = () => ownership.SetNewOwner("new-consumer-id", 2, timestampUtc);

        // Assert
        act.Should()
            .Throw<BadRequestException>()
            .WithMessage("The epoch of previous partition owner is not correct");
    }

    [Fact]
    public void SetNewOwner_ShouldUpdateOwnerEpochOwnerConsumerIdAndTimestampUtc_WhenArgumentsAreValid()
    {
        // Arrange
        var ownership = new ConsumerGroupPartitionOwnership(
            "consumer-group",
            "topic",
            0,
            1,
            "consumer-id",
            new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.Zero));

        var timestampUtc = new DateTimeOffset(2026, 1, 1, 11, 0, 0, TimeSpan.Zero);

        // Act
        ownership.SetNewOwner("new-consumer-id", 1, timestampUtc);

        // Assert
        ownership.OwnerEpoch.Should().Be(2);
        ownership.OwnerConsumerId.Should().Be("new-consumer-id");
        ownership.TimestampUtc.Should().Be(timestampUtc);
    }
}