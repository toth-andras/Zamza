using FluentAssertions;
using Zamza.Server.Models.ConsumerApi.Commit;
using Zamza.Server.Models.Exceptions;

namespace Zamza.Server.UnitTests.Models;

public sealed class ProcessedMessageTests
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
        const long offset = 10;

        // Act
        var act = () => new ProcessedMessage(topic!, partition, offset);

        // Assert
        act.Should()
            .Throw<BadRequestException>()
            .WithMessage("Processed message Topic cannot be empty");
    }

    [Fact]
    public void Constructor_ShouldSetProperties_WhenArgumentsAreValid()
    {
        // Arrange
        const string topic = "topic";
        const int partition = 1;
        const long offset = 10;

        // Act
        var processedMessage = new ProcessedMessage(topic, partition, offset);

        // Assert
        processedMessage.Topic.Should().Be(topic);
        processedMessage.Partition.Should().Be(partition);
        processedMessage.Offset.Should().Be(offset);
    }
}