using FluentAssertions;
using Moq;
using Zamza.Server.Application.ConsumerApi.Leave;
using Zamza.Server.Application.ConsumerApi.Leave.Models;
using Zamza.Server.DataAccess.Repositories.ConsumerHeartbeatRepository;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository;
using Zamza.Server.UnitTests.Utils;

namespace Zamza.Server.UnitTests.Application.ConsumerApi;


public sealed class LeaveServiceTests
{
    private readonly Mock<IPartitionOwnershipRepository> _partitionOwnershipRepositoryMock = new();
    private readonly Mock<IConsumerHeartbeatRepository> _consumerHeartbeatRepositoryMock = new();
    private readonly LoggerFake<LeaveService> _loggerFake = new();

    private readonly LeaveService _sut;

    public LeaveServiceTests()
    {
        _sut = new LeaveService(
            _partitionOwnershipRepositoryMock.Object, 
            _consumerHeartbeatRepositoryMock.Object, 
            _loggerFake);
    }

    [Fact]
    public async Task Leave_Success()
    {
        // Arrange
        SetupPartitionOwnershipRepositoryForSuccessfulDeleteConsumerOwnership();
        SetupConsumerHeartbeatRepositoryForSuccessfulDeleteConsumer();

        const string consumerId = "consumer-1";
        const string consumerGroup = "consumer-group-1";
        var timestamp = DateTimeOffset.Parse("01-01-2026").ToUniversalTime();
        
        // Act
        var request = new LeaveRequest(consumerId, consumerGroup, timestamp);
        await _sut.Leave(request, CancellationToken.None);
        
        // Assert
        _partitionOwnershipRepositoryMock.Verify(
            mock => mock.DeleteConsumerOwnerships(
                consumerId,
                consumerGroup,
                timestamp,
                It.IsAny<CancellationToken>()),
            Times.Once);

        _loggerFake.LoggedMessages.Should().Contain(
            $"Consumer \'{consumerId}\' has left consumer group \'{consumerGroup}\'");
    }

    private void SetupPartitionOwnershipRepositoryForSuccessfulDeleteConsumerOwnership()
    {
        _partitionOwnershipRepositoryMock
            .Setup(mock => mock.DeleteConsumerOwnerships(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<DateTimeOffset>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
    }

    private void SetupConsumerHeartbeatRepositoryForSuccessfulDeleteConsumer()
    {
        _consumerHeartbeatRepositoryMock
            .Setup(mock => mock.DeleteConsumer(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
    }
}