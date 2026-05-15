using FluentAssertions;
using Zamza.Server.Models.ConsumerApi.Fetch;
using Zamza.Server.Models.Exceptions;

namespace Zamza.Server.UnitTests.Models;

public sealed class FetchedPartitionTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\t")]
    public void Constructor_ShouldThrowBadRequestException_WhenTopicIsNullOrWhiteSpace(string? topic)
    {
        // Arrange
        const int partition = 1;
        const long ownershipEpoch = 2;
        const long kafkaOffset = 10;

        // Act
        var act = () => new FetchedPartition(
            topic!,
            partition,
            ownershipEpoch,
            kafkaOffset);

        // Assert
        act.Should()
            .Throw<BadRequestException>()
            .WithMessage("Fetched partition topic cannot be empty");
    }

    [Fact]
    public void Constructor_ShouldSetProperties_WhenArgumentsAreValid()
    {
        // Arrange
        const string topic = "topic";
        const int partition = 1;
        const long ownershipEpoch = 2;
        const long kafkaOffset = 10;

        // Act
        var fetchedPartition = new FetchedPartition(
            topic,
            partition,
            ownershipEpoch,
            kafkaOffset);

        // Assert
        fetchedPartition.Topic.Should().Be(topic);
        fetchedPartition.Partition.Should().Be(partition);
        fetchedPartition.OwnershipEpoch.Should().Be(ownershipEpoch);
        fetchedPartition.KafkaOffset.Should().Be(kafkaOffset);
    }
}