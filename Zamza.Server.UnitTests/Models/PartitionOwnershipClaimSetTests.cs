using FluentAssertions;
using Zamza.Server.Models.ConsumerApi.ClaimPartitionOwnership;
using Zamza.Server.Models.Exceptions;

namespace Zamza.Server.UnitTests.Models;

public sealed class PartitionOwnershipClaimSetTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\t")]
    public void Constructor_ShouldThrowBadRequestException_WhenConsumerIdIsNullOrWhiteSpace(string? consumerId)
    {
        // Arrange
        const string consumerGroup = "consumer-group";
        var timestamp = new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.Zero);

        // Act
        var act = () => new PartitionOwnershipClaimSet(consumerId!, consumerGroup, [], timestamp);

        // Assert
        act.Should()
            .Throw<BadRequestException>()
            .WithMessage("ConsumerId and consumer group in partition claims cannot be empty");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\t")]
    public void Constructor_ShouldThrowBadRequestException_WhenConsumerGroupIsNullOrWhiteSpace(string? consumerGroup)
    {
        // Arrange
        const string consumerId = "consumer-id";
        var timestamp = new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.Zero);

        // Act
        var act = () => new PartitionOwnershipClaimSet(consumerId, consumerGroup!, [], timestamp);

        // Assert
        act.Should()
            .Throw<BadRequestException>()
            .WithMessage("ConsumerId and consumer group in partition claims cannot be empty");
    }

    [Fact]
    public void Constructor_ShouldThrowBadRequestException_WhenTimestampIsNotUtc()
    {
        // Arrange
        const string consumerId = "consumer-id";
        const string consumerGroup = "consumer-group";
        var timestamp = new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.FromHours(1));

        // Act
        var act = () => new PartitionOwnershipClaimSet(consumerId, consumerGroup, [], timestamp);

        // Assert
        act.Should()
            .Throw<BadRequestException>()
            .WithMessage("The timestamp for partitions claim must be provided in UTC");
    }

    [Fact]
    public void Constructor_ShouldSetProperties_WhenArgumentsAreValid()
    {
        // Arrange
        const string consumerId = "consumer-id";
        const string consumerGroup = "consumer-group";
        var partitions = new[]
        {
            new ClaimedPartition("topic-1", 0, 1),
            new ClaimedPartition("topic-1", 1, 1)
        };
        var timestamp = new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.Zero);

        // Act
        var claimSet = new PartitionOwnershipClaimSet(consumerId, consumerGroup, partitions, timestamp);

        // Assert
        claimSet.ConsumerId.Should().Be(consumerId);
        claimSet.ConsumerGroup.Should().Be(consumerGroup);
        claimSet.Partitions.Should().BeEquivalentTo(partitions);
        claimSet.TimestampUtc.Should().Be(timestamp);
    }

    [Fact]
    public void Constructor_ShouldRemoveDuplicatePartitions_WhenPartitionsHaveSameTopicAndPartition()
    {
        // Arrange
        const string consumerId = "consumer-id";
        const string consumerGroup = "consumer-group";
        var firstPartition = new ClaimedPartition("topic-1", 0, 1);
        var duplicatePartition = new ClaimedPartition("topic-1", 0, 1);
        var uniquePartition = new ClaimedPartition("topic-1", 1, 1);
        var partitions = new[]
        {
            firstPartition,
            duplicatePartition,
            uniquePartition
        };
        var timestamp = new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.Zero);

        // Act
        var claimSet = new PartitionOwnershipClaimSet(consumerId, consumerGroup, partitions, timestamp);

        // Assert
        claimSet.Partitions.Should().HaveCount(2);
        claimSet.Partitions.Should().Contain(firstPartition);
        claimSet.Partitions.Should().Contain(uniquePartition);
    }
}