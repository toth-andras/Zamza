using System.Data;
using FluentAssertions;
using Moq;
using Zamza.Server.Application.ConsumerApi.ClaimPartitionOwnership;
using Zamza.Server.Application.ConsumerApi.ClaimPartitionOwnership.Models;
using Zamza.Server.DataAccess.Common.ConnectionsManagement;
using Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;
using Zamza.Server.DataAccess.Repositories.ConsumerHeartbeatRepository;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.Models;
using Zamza.Server.Models.ConsumerApi.ClaimPartitionOwnership;
using Zamza.Server.Models.ConsumerApi.Common;
using Zamza.Server.Models.ConsumerApi.Monitoring;
using Zamza.Server.UnitTests.Utils;

namespace Zamza.Server.UnitTests.Application.ConsumerApi;

public sealed class ClaimPartitionOwnershipServiceTests
{
    private readonly Mock<IDbConnectionsManager> _dbConnectionsManagerMock = new();
    private readonly Mock<IPartitionOwnershipRepository> _partitionOwnershipRepositoryMock = new();
    private readonly Mock<IConsumerHeartbeatRepository> _consumerHeartbeatRepositoryMock = new();
    private readonly LoggerFake<ClaimPartitionOwnershipService> _loggerFake = new();

    private readonly ClaimPartitionOwnershipService _sut;

    public ClaimPartitionOwnershipServiceTests()
    {
        _dbConnectionsManagerMock
            .Setup(mock => mock.BeginTransaction(
                It.IsAny<IsolationLevel>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Mock.Of<IDbTransactionFrame>());
        
        _sut = new ClaimPartitionOwnershipService(
            _dbConnectionsManagerMock.Object,
            _partitionOwnershipRepositoryMock.Object,
            _consumerHeartbeatRepositoryMock.Object,
            _loggerFake);
    }

    [Fact]
    public async Task ClaimPartitionOwnership_SaveConsumerHeartbeat()
    {
        // Arrange
        const string consumerId = "consumer-1";
        const string consumerGroup = "consumer-group";
        
        SetupConsumerHeartbeatRepositoryForSuccessfulHeartbeatSave();
        SetupPartitionOwnershipRepositoryForSuccessfulPartitionLock();
        SetupPartitionOwnershipResponse_GetForConsumerGroup(
            new ConsumerGroupPartitionOwnershipSet(consumerGroup, partitionOwnerships: []));
        SetupPartitionOwnershipResponseForSuccessfulUpsert();
        
        // Act
        var requestTimestamp = DateTimeOffset.Parse("2026-01-01").ToUniversalTime();
        var partitionOwnershipClaims = new PartitionOwnershipClaimSet(
            consumerId,
            consumerGroup,
            partitions: [],
            requestTimestamp);

        await _sut.ClaimPartitionOwnership(
            new ClaimPartitionOwnershipRequest(partitionOwnershipClaims),
            CancellationToken.None);
        
        // Assert
        _consumerHeartbeatRepositoryMock.Verify(
            mock => mock.SaveHeartbeat(
                It.Is<ConsumerHeartbeat>(hb => 
                    hb.ConsumerId == consumerId &&
                    hb.ConsumerGroup == consumerGroup &&
                    hb.TimestampUtc == requestTimestamp),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task ClaimPartitionOwnership_ReturnObsoleteOwnership_IfCorrectCurrentOwnerIsNotProvided()
    {
        // Arrange
        SetupConsumerHeartbeatRepositoryForSuccessfulHeartbeatSave();
        SetupPartitionOwnershipRepositoryForSuccessfulPartitionLock();
        
        const string consumerGroup = "consumer-group";
        const string topic =  "topic";
        const int partition = 2;
        const long currentPartitionOwnership = 4;

        var partitionOwnership = new ConsumerGroupPartitionOwnership(
            consumerGroup,
            topic,
            partition,
            currentPartitionOwnership,
            "owner",
            DateTimeOffset.Parse("2026-01-01").ToUniversalTime());

        var currentPartitionOwnershipForConsumerGroup = new ConsumerGroupPartitionOwnershipSet(
            consumerGroup,
            [partitionOwnership]);
        
        SetupPartitionOwnershipResponse_GetForConsumerGroup(currentPartitionOwnershipForConsumerGroup);

        var partitionClaim = new ClaimedPartition(
            topic,
            partition,
            currentPartitionOwnership - 1);
        
        var partitionOwnershipClaims = new PartitionOwnershipClaimSet(
            "consumer-id",
            consumerGroup,
            partitions: [partitionClaim],
            DateTimeOffset.Parse("2026-01-01").ToUniversalTime());
        
        // Act
        var result = await _sut.ClaimPartitionOwnership(
            new ClaimPartitionOwnershipRequest(partitionOwnershipClaims),
            CancellationToken.None);

        // Assert
        result.Result.Should().Be(OwnershipClaimResult.Obsolete);
        result.ConsumerGroupPartitionOwnerships.Should().BeEquivalentTo(currentPartitionOwnershipForConsumerGroup);
        
        _partitionOwnershipRepositoryMock.Verify(
            mock => mock.Upsert(
                It.IsAny<IDbTransactionFrame>(), 
                It.IsAny<ConsumerGroupPartitionOwnershipSet>(), 
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task ClaimPartitionOwnership_ReturnObsoleteOwnership_IfPartitionIsNewAndOwnershipEpochIsInvalid()
    {
        // Arrange
        SetupConsumerHeartbeatRepositoryForSuccessfulHeartbeatSave();
        SetupPartitionOwnershipRepositoryForSuccessfulPartitionLock();
        
        const string consumerGroup = "consumer-group";
        const string topic =  "topic";
        const int partition = 2;

        var currentPartitionOwnershipForConsumerGroup = new ConsumerGroupPartitionOwnershipSet(
            consumerGroup,
            partitionOwnerships: []);
        
        SetupPartitionOwnershipResponse_GetForConsumerGroup(currentPartitionOwnershipForConsumerGroup);

        var partitionClaim = new ClaimedPartition(
            topic,
            partition,
            2);
        
        var partitionOwnershipClaims = new PartitionOwnershipClaimSet(
            "consumer-id",
            consumerGroup,
            partitions: [partitionClaim],
            DateTimeOffset.Parse("2026-01-01").ToUniversalTime());
        
        // Act
        var result = await _sut.ClaimPartitionOwnership(
            new ClaimPartitionOwnershipRequest(partitionOwnershipClaims),
            CancellationToken.None);

        // Assert
        result.Result.Should().Be(OwnershipClaimResult.Obsolete);
        result.ConsumerGroupPartitionOwnerships.Should().BeEquivalentTo(currentPartitionOwnershipForConsumerGroup);
        
        _partitionOwnershipRepositoryMock.Verify(
            mock => mock.Upsert(
                It.IsAny<IDbTransactionFrame>(), 
                It.IsAny<ConsumerGroupPartitionOwnershipSet>(), 
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task ClaimPartitionOwnership_SaveNewPartitionOwner_ForRegisteredPartition()
    {
        // Arrange
        SetupConsumerHeartbeatRepositoryForSuccessfulHeartbeatSave();
        SetupPartitionOwnershipRepositoryForSuccessfulPartitionLock();
        
        const string consumerGroup = "consumer-group";
        const string topic =  "topic";
        const int partition = 2;
        const long currentPartitionOwnership = 4;

        var partitionOwnership = new ConsumerGroupPartitionOwnership(
            consumerGroup,
            topic,
            partition,
            currentPartitionOwnership,
            "owner",
            DateTimeOffset.Parse("2026-01-01").ToUniversalTime());

        var currentPartitionOwnershipForConsumerGroup = new ConsumerGroupPartitionOwnershipSet(
            consumerGroup,
            [partitionOwnership]);
        
        SetupPartitionOwnershipResponse_GetForConsumerGroup(currentPartitionOwnershipForConsumerGroup);

        const string newPartitionOwnerConsumerId = "new-consumer-id";

        var claimTimestamp = DateTimeOffset.Parse("2026-01-01").ToUniversalTime();
        var partitionOwnershipClaims = new PartitionOwnershipClaimSet(
            newPartitionOwnerConsumerId,
            consumerGroup,
            partitions: [
                new ClaimedPartition(
                    topic,
                    partition,
                    currentPartitionOwnership)
            ],
            claimTimestamp);
        
        // Act
        var result = await _sut.ClaimPartitionOwnership(
            new ClaimPartitionOwnershipRequest(partitionOwnershipClaims),
            CancellationToken.None);

        // Assert
        result.Result.Should().Be(OwnershipClaimResult.Ok);
        result.ConsumerGroupPartitionOwnerships.Should().BeEquivalentTo(currentPartitionOwnershipForConsumerGroup);
        
        _partitionOwnershipRepositoryMock.Verify(
            mock => mock.Upsert(
                It.IsAny<IDbTransactionFrame>(), 
                It.Is<ConsumerGroupPartitionOwnershipSet>(set =>
                    set.ConsumerGroup == consumerGroup &&
                    set.PartitionCount == 1 &&
                    set.Single().ConsumerGroup == consumerGroup &&
                    set.Single().Topic == topic &&
                    set.Single().Partition == partition &&
                    set.Single().OwnerConsumerId == newPartitionOwnerConsumerId &&
                    set.Single().OwnerEpoch == currentPartitionOwnership + 1 &&
                    set.Single().TimestampUtc == claimTimestamp), 
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task ClaimPartitionOwnership_SaveNewPartitionOwner_ForNewPartition()
    {
         // Arrange
        SetupConsumerHeartbeatRepositoryForSuccessfulHeartbeatSave();
        SetupPartitionOwnershipRepositoryForSuccessfulPartitionLock();
        
        const string consumerGroup = "consumer-group";
        const string topic =  "topic";
        const int partition = 2;

        var currentPartitionOwnershipForConsumerGroup = new ConsumerGroupPartitionOwnershipSet(
            consumerGroup,
            partitionOwnerships: []);
        
        SetupPartitionOwnershipResponse_GetForConsumerGroup(currentPartitionOwnershipForConsumerGroup);

        const string newPartitionOwnerConsumerId = "new-consumer-id";

        const long newPartitionOwnerEpoch = 0;
        var claimTimestamp = DateTimeOffset.Parse("2026-01-01").ToUniversalTime();
        var partitionOwnershipClaims = new PartitionOwnershipClaimSet(
            newPartitionOwnerConsumerId,
            consumerGroup,
            partitions: [
                new ClaimedPartition(
                    topic,
                    partition,
                    newPartitionOwnerEpoch)
            ],
            claimTimestamp);
        
        // Act
        var result = await _sut.ClaimPartitionOwnership(
            new ClaimPartitionOwnershipRequest(partitionOwnershipClaims),
            CancellationToken.None);

        // Assert
        result.Result.Should().Be(OwnershipClaimResult.Ok);
        result.ConsumerGroupPartitionOwnerships.Should().BeEquivalentTo(currentPartitionOwnershipForConsumerGroup);
        
        _partitionOwnershipRepositoryMock.Verify(
            mock => mock.Upsert(
                It.IsAny<IDbTransactionFrame>(), 
                It.Is<ConsumerGroupPartitionOwnershipSet>(set =>
                    set.ConsumerGroup == consumerGroup &&
                    set.PartitionCount == 1 &&
                    set.Single().ConsumerGroup == consumerGroup &&
                    set.Single().Topic == topic &&
                    set.Single().Partition == partition &&
                    set.Single().OwnerConsumerId == newPartitionOwnerConsumerId &&
                    set.Single().OwnerEpoch == 1 &&
                    set.Single().TimestampUtc == claimTimestamp), 
                It.IsAny<CancellationToken>()),
            Times.Once);
    }
    
    private void SetupConsumerHeartbeatRepositoryForSuccessfulHeartbeatSave()
    {
        _consumerHeartbeatRepositoryMock
            .Setup(mock => mock.SaveHeartbeat(
                It.IsAny<ConsumerHeartbeat>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
    }
    
    private void SetupPartitionOwnershipRepositoryForSuccessfulPartitionLock()
    {
        _partitionOwnershipRepositoryMock
            .Setup(mock => mock.LockPartitions(
                It.IsAny<IDbTransactionFrame>(),
                It.IsAny<string>(),
                It.IsAny<IReadOnlyList<PartitionToLock>>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
    }

    private void SetupPartitionOwnershipResponse_GetForConsumerGroup(ConsumerGroupPartitionOwnershipSet returnValue)
    {
        _partitionOwnershipRepositoryMock
            .Setup(mock => mock.GetForConsumerGroup(
                It.IsAny<IDbTransactionFrame>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(returnValue);
    }

    private void SetupPartitionOwnershipResponseForSuccessfulUpsert()
    {
        _partitionOwnershipRepositoryMock
            .Setup(mock => mock.Upsert(
                It.IsAny<IDbTransactionFrame>(),
                It.IsAny<ConsumerGroupPartitionOwnershipSet>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
    }
}