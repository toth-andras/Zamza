using FluentAssertions;
using Moq;
using Zamza.Server.Application.ConsumerApi.Fetch;
using Zamza.Server.Application.ConsumerApi.Fetch.Models;
using Zamza.Server.DataAccess.Repositories.ConsumerHeartbeatRepository;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository;
using Zamza.Server.DataAccess.Repositories.RetryQueueRepository;
using Zamza.Server.Models.ConsumerApi.Common;
using Zamza.Server.Models.ConsumerApi.Fetch;
using Zamza.Server.Models.ConsumerApi.Monitoring;

namespace Zamza.Server.UnitTests.Application.ConsumerApi;

public sealed class FetchServiceTests
{
    private readonly Mock<IPartitionOwnershipRepository> _partitionOwnershipRepositoryMock = new();
    private readonly Mock<IRetryQueueRepository> _retryQueueRepositoryMock = new();
    private readonly Mock<IConsumerHeartbeatRepository> _consumerHeartbeatRepositoryMock = new();
    
    private readonly FetchService _sut;

    public FetchServiceTests()
    {
        _sut = new FetchService(
            _partitionOwnershipRepositoryMock.Object,
            _retryQueueRepositoryMock.Object,
            _consumerHeartbeatRepositoryMock.Object);
    }

    [Fact]
    public async Task Fetch_SaveConsumerHeartbeat()
    {
        // Arrange
        const string consumerId = "consumer-1";
        const string consumerGroup = "consumer-group-1";
        
        SetupConsumerHeartbeatRepositoryForSuccessfulHeartbeatSave();

        var partitionOwnershipSet = new ConsumerGroupPartitionOwnershipSet(consumerGroup, partitionOwnerships: []);
        SetupPartitionOwnershipRepositoryResponse(partitionOwnershipSet);
        
        SetupRetryQueueRepositoryResponse(messages: []);
        
        var timestamp = DateTimeOffset.Parse("2026-01-01").ToUniversalTime();
        
        // Act
        var request = new FetchRequest(consumerId, consumerGroup, [], limit: 5, timestamp);
        await _sut.Fetch(request, CancellationToken.None);
        
        // Assert
        _consumerHeartbeatRepositoryMock.Verify(mock => mock.SaveHeartbeat(
                It.Is<ConsumerHeartbeat>(hb => 
                    hb.ConsumerId == consumerId &&
                    hb.ConsumerGroup == consumerGroup &&
                    hb.TimestampUtc ==  timestamp),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task Fetch_ReturnObsoleteOwnership_IfPartitionOwnershipObsolete()
    {
        // Arrange
        SetupConsumerHeartbeatRepositoryForSuccessfulHeartbeatSave();
        
        const string consumerGroup = "consumer-group-1";
        const string topic = "topic-1";
        const int partition = 2;
        const long currentOwnershipEpoch = 4;
        var currentPartitionOwnershipForConsumerGroup = new ConsumerGroupPartitionOwnershipSet(
            consumerGroup,
            [
                new ConsumerGroupPartitionOwnership(
                    consumerGroup,
                    topic,
                    partition,
                    currentOwnershipEpoch,
                    "consumer-id",
                    DateTimeOffset.Parse("2026-01-01").ToUniversalTime())
            ]);
        
        SetupPartitionOwnershipRepositoryResponse(currentPartitionOwnershipForConsumerGroup);

        var partitionFetched = new FetchedPartition(
            topic, 
            partition, 
            currentOwnershipEpoch - 1, 
            kafkaOffset: 2);

        var request = new FetchRequest(
            consumerId: "consumer-1",
            consumerGroup,
            partitions: [partitionFetched],
            limit: 5,
            DateTimeOffset.Parse("2026-01-01").ToUniversalTime());
        
        // Act
        var result = await _sut.Fetch(request, CancellationToken.None);
        
        // Assert
        result.Result.Should().Be(FetchResult.ObsoleteOwnership);
        result.ConsumerGroupPartitionOwnerships.Should().BeEquivalentTo(currentPartitionOwnershipForConsumerGroup);
        result.FetchedMessages.Should().BeNull();
        
        _retryQueueRepositoryMock.VerifyNoOtherCalls();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public async Task Fetch_ReturnObsoleteOwnership_IfPartitionIsNotRegisteredButFetched(long ownershipEpoch)
    {
        // Arrange
        SetupConsumerHeartbeatRepositoryForSuccessfulHeartbeatSave();
        
        const string consumerGroup = "consumer-group-1";
        const string topic = "topic-1";
        const int partition = 2;
        var currentPartitionOwnershipForConsumerGroup = new ConsumerGroupPartitionOwnershipSet(
            consumerGroup,
            []);
        
        SetupPartitionOwnershipRepositoryResponse(currentPartitionOwnershipForConsumerGroup);

        var partitionFetched = new FetchedPartition(
            topic, 
            partition, 
            ownershipEpoch: ownershipEpoch, 
            kafkaOffset: 2);

        var request = new FetchRequest(
            consumerId: "consumer-1",
            consumerGroup,
            partitions: [partitionFetched],
            limit: 5,
            DateTimeOffset.Parse("2026-01-01").ToUniversalTime());
        
        // Act
        var result = await _sut.Fetch(request, CancellationToken.None);
        
        // Assert
        result.Result.Should().Be(FetchResult.ObsoleteOwnership);
        result.ConsumerGroupPartitionOwnerships.Should().BeEquivalentTo(currentPartitionOwnershipForConsumerGroup);
        result.FetchedMessages.Should().BeNull();
        
        _retryQueueRepositoryMock.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task Fetch_ReturnMessages()
    {
        // Arrange
        SetupConsumerHeartbeatRepositoryForSuccessfulHeartbeatSave();
        
        const string consumerGroup = "consumer-group-1";
        const string topic = "topic-1";
        const int partition = 2;
        const long currentOwnershipEpoch = 4;
        var currentPartitionOwnershipForConsumerGroup = new ConsumerGroupPartitionOwnershipSet(
            consumerGroup,
            [
                new ConsumerGroupPartitionOwnership(
                    consumerGroup,
                    topic,
                    partition,
                    currentOwnershipEpoch,
                    "consumer-id",
                    DateTimeOffset.Parse("2026-01-01").ToUniversalTime())
            ]);
        SetupPartitionOwnershipRepositoryResponse(currentPartitionOwnershipForConsumerGroup);

        List<FetchedMessage> fetchedMessages = 
        [
            new FetchedMessage(
                topic,
                partition,
                Offset: 5,
                Headers: new Dictionary<string, byte[]>(),
                "Message key"u8.ToArray(),
                "Message value"u8.ToArray(),
                DateTimeOffset.Parse("2026-01-01"),
                MaxRetriesCount: 5,
                RetriesCount: 1,
                ProcessingDeadlineUtc: null),
            
            new FetchedMessage(
                topic,
                partition,
                Offset: 10,
                Headers: new Dictionary<string, byte[]>(),
                "Message key 2"u8.ToArray(),
                "Message value 2"u8.ToArray(),
                DateTimeOffset.Parse("2026-01-02"),
                MaxRetriesCount: 10,
                RetriesCount: 10,
                ProcessingDeadlineUtc: DateTimeOffset.Parse("2026-01-02"))
        ];
        SetupRetryQueueRepositoryResponse(fetchedMessages);
        
        var request = new FetchRequest(
            consumerId: "consumer-1",
            consumerGroup,
            partitions: [
                new FetchedPartition(topic, partition, currentOwnershipEpoch, kafkaOffset: 2)
            ],
            limit: 5,
            DateTimeOffset.Parse("2026-01-01").ToUniversalTime());
        
        // Act
        var result = await _sut.Fetch(request, CancellationToken.None);
        
        // Assert
        result.Result.Should().Be(FetchResult.Ok);
        result.ConsumerGroupPartitionOwnerships.Should().BeEquivalentTo(currentPartitionOwnershipForConsumerGroup);
        result.FetchedMessages.Should().BeEquivalentTo(fetchedMessages);
    }

    private void SetupConsumerHeartbeatRepositoryForSuccessfulHeartbeatSave()
    {
        _consumerHeartbeatRepositoryMock
            .Setup(mock => mock.SaveHeartbeat(
                It.IsAny<ConsumerHeartbeat>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
    }
    private void SetupPartitionOwnershipRepositoryResponse(ConsumerGroupPartitionOwnershipSet returnValue)
    {
        _partitionOwnershipRepositoryMock
            .Setup(mock => mock.GetForConsumerGroup(
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(returnValue);
    }

    private void SetupRetryQueueRepositoryResponse(params List<FetchedMessage> messages)
    {
        _retryQueueRepositoryMock
            .Setup(mock => mock.GetForFetch(
                It.IsAny<string>(),
                It.IsAny<IReadOnlyCollection<FetchedPartition>>(),
                It.IsAny<int>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(messages);
    }
}