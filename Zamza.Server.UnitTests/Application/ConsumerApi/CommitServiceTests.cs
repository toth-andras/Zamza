using System.Data;
using FluentAssertions;
using Moq;
using Zamza.Server.Application.ConsumerApi.Commit;
using Zamza.Server.Application.ConsumerApi.Commit.Models;
using Zamza.Server.DataAccess.Common.ConnectionsManagement;
using Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;
using Zamza.Server.DataAccess.Repositories.CommonModels;
using Zamza.Server.DataAccess.Repositories.ConsumerHeartbeatRepository;
using Zamza.Server.DataAccess.Repositories.DLQRepository;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.Models;
using Zamza.Server.DataAccess.Repositories.RetryQueueRepository;
using Zamza.Server.Models.ConsumerApi.Commit;
using Zamza.Server.Models.ConsumerApi.Common;
using Zamza.Server.Models.ConsumerApi.Monitoring;

namespace Zamza.Server.UnitTests.Application.ConsumerApi;

public sealed class CommitServiceTests
{
    private readonly Mock<IDbConnectionsManager> _dbConnectionsManagerMock = new();
    private readonly Mock<IPartitionOwnershipRepository> _partitionOwnershipRepositoryMock = new();
    private readonly Mock<IRetryQueueRepository> _retryQueueRepositoryMock = new();
    private readonly Mock<IDLQRepository> _dlqRepositoryMock = new();
    private readonly Mock<IConsumerHeartbeatRepository> _consumerHeartbeatRepositoryMock = new();
    
    private readonly CommitService _sut;
    
    public CommitServiceTests()
    {
        _dbConnectionsManagerMock
            .Setup(mock => mock.BeginTransaction(
                It.IsAny<IsolationLevel>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Mock.Of<IDbTransactionFrame>());
        
        _sut = new CommitService(
            _dbConnectionsManagerMock.Object,
            _partitionOwnershipRepositoryMock.Object,
            _retryQueueRepositoryMock.Object,
            _consumerHeartbeatRepositoryMock.Object,
            _dlqRepositoryMock.Object);
    }

    [Fact]
    public async Task Commit_SaveConsumerHeartbeat()
    {
        // Arrange
        const string consumerId = "consumer-1";
        const string consumerGroup = "consumer-group";
        var commitTimestamp = DateTimeOffset.Parse("2026-01-01").ToUniversalTime();
        
        SetupConsumerHeartbeatRepositoryForSuccessfulHeartbeatSave();
        SetupPartitionOwnershipRepositoryForSuccessfulPartitionLock();
        SetupPartitionOwnershipResponse_GetForConsumerGroup(
            new ConsumerGroupPartitionOwnershipSet(consumerGroup, partitionOwnerships: []));
        SetupRetryQueueRepositoryForSuccessfulDelete();
        SetupDLQRepositoryForSuccessfulDelete();
        SetupRetryQueueRepositoryForSuccessfulUpsert();
        SetupDLQRepositoryForSuccessfulUpsert();
        
        // Act
        var request = new CommitRequest(
            consumerId,
            consumerGroup,
            Partitions: [],
            ProcessedMessages: [],
            RetryableMessages: [],
            FailedMessages: [],
            commitTimestamp);

        await _sut.Commit(request, CancellationToken.None);
        
        // Assert
        _consumerHeartbeatRepositoryMock.Verify(
            mock => mock.SaveHeartbeat(
                It.Is<ConsumerHeartbeat>(hb => 
                    hb.ConsumerId == consumerId &&
                    hb.ConsumerGroup == consumerGroup &&
                    hb.TimestampUtc == commitTimestamp),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task Commit_FilterOutMessagesFromPartitionsWithIrrelevantOwnership()
    {
        // Arrange
        const string consumerId = "consumer-1";
        const string consumerGroup = "consumer-group";
        var commitTimestamp = DateTimeOffset.Parse("2026-01-02").ToUniversalTime();
        
        const string topic = "topic-1";
        const int partition = 3;
        const long currentPartitionOwnership = 4;
        
        SetupConsumerHeartbeatRepositoryForSuccessfulHeartbeatSave();
        SetupPartitionOwnershipRepositoryForSuccessfulPartitionLock();
        var partitionOwnershipForConsumerGroup = new ConsumerGroupPartitionOwnershipSet(
            consumerGroup,
            partitionOwnerships:
            [
                new ConsumerGroupPartitionOwnership(
                    consumerGroup,
                    topic,
                    partition,
                    currentPartitionOwnership,
                    "owner",
                    DateTimeOffset.Parse("2026-01-01").ToUniversalTime())
            ]);
        SetupPartitionOwnershipResponse_GetForConsumerGroup(partitionOwnershipForConsumerGroup);
        SetupRetryQueueRepositoryForSuccessfulDelete();
        SetupDLQRepositoryForSuccessfulDelete();
        SetupRetryQueueRepositoryForSuccessfulUpsert();
        SetupDLQRepositoryForSuccessfulUpsert();
        
        // Act
        var processedMessage = new ProcessedMessage(topic, partition, 2);
        var retryableMessage = new RetryableMessage(
            topic,
            partition,
            3,
            new Dictionary<string, byte[]>(),
            "key"u8.ToArray(),
            "value"u8.ToArray(),
            DateTimeOffset.Parse("2026-01-01").ToUniversalTime(),
            maxRetriesCount: 5,
            retriesCount: 2,
            processingDeadlineUtc: null,
            nextRetryAfterMs: 100);
        var failedMessage = new FailedMessage(
            topic,
            partition,
            4,
            new Dictionary<string, byte[]>(),
            "key"u8.ToArray(),
            "value"u8.ToArray(),
            DateTimeOffset.Parse("2026-01-01").ToUniversalTime(),
            retriesCount: 2,
            DateTimeOffset.Parse("2026-01-01").ToUniversalTime());
        
        var request = new CommitRequest(
            consumerId,
            consumerGroup,
            Partitions: [
                new CommitedPartition(topic, partition, currentPartitionOwnership - 1)
            ],
            ProcessedMessages: [processedMessage],
            RetryableMessages: [retryableMessage],
            FailedMessages: [failedMessage],
            commitTimestamp);

        var result = await _sut.Commit(request, CancellationToken.None);
        
        // Assert
        result.ConsumerGroupPartitionOwnerships.Should().BeEquivalentTo(partitionOwnershipForConsumerGroup);
        result.PartitionsWithIrrelevantOwnership.Should().Contain(p => p.Topic == topic && p.Partition == partition);
        
        _retryQueueRepositoryMock.Verify(
            mock => mock.Upsert(
                It.IsAny<IDbTransactionFrame>(),
                consumerGroup,
                It.Is<IReadOnlyCollection<RetryableMessage>>(messages => messages.Contains(retryableMessage)),
                It.IsAny<CancellationToken>()),
            Times.Never);
        
        _dlqRepositoryMock.Verify(
            mock => mock.Upsert(
                It.IsAny<IDbTransactionFrame>(),
                consumerGroup,
                It.Is<IReadOnlyCollection<FailedMessage>>(messages => messages.Contains(failedMessage)),
                It.IsAny<CancellationToken>()),
            Times.Never);
        
        _retryQueueRepositoryMock.Verify(
            mock => mock.Delete(
                It.IsAny<IDbTransactionFrame>(),
                It.IsAny<string>(),
                It.Is<IReadOnlyCollection<MessageToDelete>>(messages =>
                    messages.Any(message => message.Topic == topic && message.Partition == partition)),
                It.IsAny<CancellationToken>()),
            Times.Never);
        
        _dlqRepositoryMock.Verify(
            mock => mock.Delete(
                It.IsAny<IDbTransactionFrame>(),
                It.IsAny<string>(),
                It.Is<IReadOnlyCollection<MessageToDelete>>(messages =>
                    messages.Any(message => message.Topic == topic && message.Partition == partition)),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task Commit_CommitMessages()
    {
         // Arrange
        const string consumerId = "consumer-1";
        const string consumerGroup = "consumer-group";
        var commitTimestamp = DateTimeOffset.Parse("2026-01-02").ToUniversalTime();
        
        const string topic = "topic-1";
        const int partition = 3;
        const long currentPartitionOwnership = 4;
        
        SetupConsumerHeartbeatRepositoryForSuccessfulHeartbeatSave();
        SetupPartitionOwnershipRepositoryForSuccessfulPartitionLock();
        var partitionOwnershipForConsumerGroup = new ConsumerGroupPartitionOwnershipSet(
            consumerGroup,
            partitionOwnerships:
            [
                new ConsumerGroupPartitionOwnership(
                    consumerGroup,
                    topic,
                    partition,
                    currentPartitionOwnership,
                    "owner",
                    DateTimeOffset.Parse("2026-01-01").ToUniversalTime())
            ]);
        SetupPartitionOwnershipResponse_GetForConsumerGroup(partitionOwnershipForConsumerGroup);
        SetupRetryQueueRepositoryForSuccessfulDelete();
        SetupDLQRepositoryForSuccessfulDelete();
        SetupRetryQueueRepositoryForSuccessfulUpsert();
        SetupDLQRepositoryForSuccessfulUpsert();
        
        // Act
        var processedMessage = new ProcessedMessage(topic, partition, 2);
        var retryableMessage = new RetryableMessage(
            topic,
            partition,
            3,
            new Dictionary<string, byte[]>(),
            "key"u8.ToArray(),
            "value"u8.ToArray(),
            DateTimeOffset.Parse("2026-01-01").ToUniversalTime(),
            maxRetriesCount: 5,
            retriesCount: 2,
            processingDeadlineUtc: null,
            nextRetryAfterMs: 100);
        var failedMessage = new FailedMessage(
            topic,
            partition,
            4,
            new Dictionary<string, byte[]>(),
            "key"u8.ToArray(),
            "value"u8.ToArray(),
            DateTimeOffset.Parse("2026-01-01").ToUniversalTime(),
            retriesCount: 2,
            DateTimeOffset.Parse("2026-01-01").ToUniversalTime());
        
        var request = new CommitRequest(
            consumerId,
            consumerGroup,
            Partitions: [
                new CommitedPartition(topic, partition, currentPartitionOwnership)
            ],
            ProcessedMessages: [processedMessage],
            RetryableMessages: [retryableMessage],
            FailedMessages: [failedMessage],
            commitTimestamp);

        var result = await _sut.Commit(request, CancellationToken.None);
        
        // Assert
        result.ConsumerGroupPartitionOwnerships.Should().BeEquivalentTo(partitionOwnershipForConsumerGroup);
        result.PartitionsWithIrrelevantOwnership.Should().BeEmpty();
        
        _retryQueueRepositoryMock.Verify(
            mock => mock.Upsert(
                It.IsAny<IDbTransactionFrame>(),
                consumerGroup,
                It.Is<IReadOnlyCollection<RetryableMessage>>(messages => messages.Contains(retryableMessage)),
                It.IsAny<CancellationToken>()),
            Times.Once);
        
        _dlqRepositoryMock.Verify(
            mock => mock.Upsert(
                It.IsAny<IDbTransactionFrame>(),
                consumerGroup,
                It.Is<IReadOnlyCollection<FailedMessage>>(messages => messages.Contains(failedMessage)),
                It.IsAny<CancellationToken>()),
            Times.Once);
        
        _retryQueueRepositoryMock.Verify(
            mock => mock.Delete(
                It.IsAny<IDbTransactionFrame>(),
                It.IsAny<string>(),
                It.Is<IReadOnlyCollection<MessageToDelete>>(messages =>
                    messages.Any(message => message.Topic == topic && message.Partition == partition) &&
                    messages.Any(message => message.Topic == failedMessage.Topic && message.Partition == failedMessage.Partition)),
                It.IsAny<CancellationToken>()),
            Times.Once);
        
        _dlqRepositoryMock.Verify(
            mock => mock.Delete(
                It.IsAny<IDbTransactionFrame>(),
                It.IsAny<string>(),
                It.Is<IReadOnlyCollection<MessageToDelete>>(messages =>
                    messages.Any(message => message.Topic == topic && message.Partition == partition) &&
                    messages.Any(message => message.Topic == processedMessage.Topic && message.Partition == processedMessage.Partition)),
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
    
    private void SetupRetryQueueRepositoryForSuccessfulDelete()
    {
        _retryQueueRepositoryMock
            .Setup(mock => mock.Upsert(
                It.IsAny<IDbTransactionFrame>(),
                It.IsAny<string>(),
                It.IsAny<IReadOnlyCollection<RetryableMessage>>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
    }
    
    private void SetupDLQRepositoryForSuccessfulDelete()
    {
        _dlqRepositoryMock
            .Setup(mock => mock.Upsert(
                It.IsAny<IDbTransactionFrame>(),
                It.IsAny<string>(),
                It.IsAny<IReadOnlyCollection<FailedMessage>>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
    }

    private void SetupRetryQueueRepositoryForSuccessfulUpsert()
    {
        _retryQueueRepositoryMock
            .Setup(mock => mock.Upsert(
                It.IsAny<IDbTransactionFrame>(),
                It.IsAny<string>(),
                It.IsAny<IReadOnlyCollection<RetryableMessage>>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
    }
    
    private void SetupDLQRepositoryForSuccessfulUpsert()
    {
        _dlqRepositoryMock.Setup(mock => mock.Upsert(
                It.IsAny<IDbTransactionFrame>(),
                It.IsAny<string>(),
                It.IsAny<IReadOnlyCollection<FailedMessage>>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
    }
}