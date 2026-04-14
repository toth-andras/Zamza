using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using Zamza.Consumer.Internal.Utils.DateTimeProvider;
using Zamza.Consumer.Internal.ZamzaServer.Exceptions;
using Zamza.Consumer.Internal.ZamzaServer.Mapping;
using Zamza.Consumer.Internal.ZamzaServer.Models;
using Zamza.ConsumerApi.V1;
using ClaimPartitionOwnershipRequest = Zamza.Consumer.Internal.ZamzaServer.Models.ClaimPartitionOwnershipRequest;
using FetchRequest = Zamza.Consumer.Internal.ZamzaServer.Models.FetchRequest;
using LeaveRequest = Zamza.Consumer.Internal.ZamzaServer.Models.LeaveRequest;
using PingRequest = Zamza.Consumer.Internal.ZamzaServer.Models.PingRequest;

namespace Zamza.Consumer.Internal.ZamzaServer;

internal sealed class ZamzaServerFacade<TKey, TValue> : IZamzaServerFacade<TKey, TValue>, IDisposable
{
    private readonly GrpcChannel _grpcChannel;
    private readonly ConsumerApiV1.ConsumerApiV1Client _grpcClient;
    private readonly IDateTimeProvider _dateTimeProvider;
    private readonly ILogger<ZamzaServerFacade<TKey, TValue>> _logger;

    public ZamzaServerFacade(
        Uri serverHost,
        IDateTimeProvider dateTimeProvider,
        ILogger<ZamzaServerFacade<TKey, TValue>> logger)
    {
        _grpcChannel = GrpcChannel.ForAddress(serverHost);
        _grpcClient = new ConsumerApiV1.ConsumerApiV1Client(_grpcChannel);
        _dateTimeProvider = dateTimeProvider;
        _logger = logger;
    }

    public async Task<ClaimPartitionOwnershipResult> ClaimPartitionOwnership(
        ClaimPartitionOwnershipRequest request,
        CancellationToken cancellationToken)
    {
        var timeout = TimeSpan.FromSeconds(3);
        
        using var scope = _logger.BeginScope(
            "[ClaimPartitionOwnership] ConsumerGroup: {ConsumerGroup}, ConsumerId: {ConsumerId}",
            request.ConsumerGroup, request.ConsumerId);

        Log.ClaimPartitionOwnership.Request(_logger, request);
        
        ClaimPartitionOwnershipResponse grpcResponse;
        try
        {
            grpcResponse = await _grpcClient
                .ClaimPartitionOwnershipAsync(
                    request.ToGrpc(), 
                    deadline: _dateTimeProvider.UtcNow.AddSeconds(timeout.TotalSeconds), 
                    cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.Unavailable)
        {
            _logger.LogError("Zamza server is not available");
            throw new ZamzaException(ZamzaErrorCode.ServerUnavailable);
        }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.InvalidArgument)
        {
            _logger.LogError(
                exception, 
                "The fetch request to Zamza server did not match the protocol: {ErrorMessage}",
                exception.Message);
            
            throw new ZamzaException(ZamzaErrorCode.InternalError);
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception, 
                "An unexpected exception occured during ClaimPartitionOwnership request to Zamza server");
            throw new ZamzaException(ZamzaErrorCode.InternalError);
        }

        var areClaimsRelevant = grpcResponse.ResultCase is ClaimPartitionOwnershipResponse.ResultOneofCase.Ok;

        var result = new  ClaimPartitionOwnershipResult(
            IsSuccessful: areClaimsRelevant,
            grpcResponse.CurrentOwnershipsForConsumerGroup.PartitionOwnerships
                .Select(ownership => ownership.ToModel())
                .ToList());
        
        Log.ClaimPartitionOwnership.Result(_logger, result);
        return result;
    }

    public async Task<FetchResult<TKey, TValue>> Fetch(
        FetchRequest request,
        CancellationToken cancellationToken)
    {
        var fetchTimeout = TimeSpan.FromSeconds(1);

        using var scope = _logger.BeginScope(
            "[Fetch] ConsumerGroup: {ConsumerGroup}, ConsumerId: {ConsumerId}",
            request.ConsumerGroup, request.ConsumerId);

        FetchResponse grpcResponse;
        try
        {
            Log.Fetch.Request(_logger, request);
            
            grpcResponse = await _grpcClient
                .FetchAsync(
                    request.ToGrpc(), 
                    deadline: _dateTimeProvider.UtcNow.AddSeconds(fetchTimeout.TotalSeconds), 
                    cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.Unavailable)
        {
            _logger.LogError("Zamza server is not available");
            throw new ZamzaException(ZamzaErrorCode.ServerUnavailable);
        }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.InvalidArgument)
        {
            _logger.LogError(
                exception, 
                "The fetch request to Zamza server did not match the protocol: {ErrorMessage}",
                exception.Message);
            throw new ZamzaException(ZamzaErrorCode.InternalError);
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception, 
                "An unexpected exception occured during fetch request to Zamza server");
            throw new ZamzaException(ZamzaErrorCode.InternalError, innerException: exception);
        }

        var consumerGroupPartitionOwnerships = grpcResponse.CurrentOwnershipsForConsumerGroup.PartitionOwnerships
            .Select(ownership => ownership.ToModel())
            .ToList();

        if (grpcResponse.ResultCase is FetchResponse.ResultOneofCase.ObsoleteOwnership)
        {
            return FetchResult<TKey, TValue>.AsPartitionOwnershipObsolete(consumerGroupPartitionOwnerships);
        }

        if (grpcResponse.ResultCase is FetchResponse.ResultOneofCase.Ok)
        {
            var messages = grpcResponse.Ok.Messages
                .Select(message => message.ToModel<TKey, TValue>())
                .ToList();
            
            var result = FetchResult<TKey, TValue>.AsOk(consumerGroupPartitionOwnerships, messages);
            
            Log.Fetch.Result(_logger, result);
            return result;
        }

        // Should not get here
        throw new NotSupportedException("The version of protocol used by Zamza server is not supported");
    }

    public async Task<CommitResult> Commit(
        CommitRequest<TKey, TValue> request,
        CancellationToken cancellationToken)
    {
        using var scope = _logger.BeginScope(
            "[Commit] ConsumerGroup: {ConsumerGroup}, ConsumerId: {ConsumerId}",
            request.ConsumerGroup, request.ConsumerId);
        
        Log.Commit.Request(_logger, request);
        
        var commitTimeout = TimeSpan.FromSeconds(3);
        try
        {
            var grpcResult= await _grpcClient
                .CommitAsync(request.ToGrpc(), 
                    deadline: _dateTimeProvider.UtcNow.AddSeconds(commitTimeout.TotalSeconds), 
                    cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            var result = grpcResult.ToModel();
            Log.Commit.Response(_logger, result);

            return result;
        }
        catch (RpcException exception) when (exception.StatusCode is StatusCode.Unavailable)
        {
            _logger.LogError("Zamza server is not available");
            throw new ZamzaException(ZamzaErrorCode.ServerUnavailable);
        }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.InvalidArgument)
        {
            _logger.LogError(exception, "Commit request did not match the protocol");
            throw new ZamzaException(ZamzaErrorCode.InternalError, innerException: exception);
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "An unexpected exception occured during Commit");
            throw new ZamzaException(ZamzaErrorCode.InternalError);
        }
    }

    public async Task<bool> Ping(
        PingRequest request,
        CancellationToken cancellationToken)
    {
        using var scope = _logger.BeginScope(
            "[Ping] ConsumerGroup: {ConsumerGroup}, ConsumerId: {ConsumerId}",
            request.ConsumerGroup, request.ConsumerId);
        
        _logger.LogDebug("Ping request to Zamza server");
        
        var timeout = TimeSpan.FromSeconds(1);
        try
        {
            await _grpcClient
                .PingAsync(
                    new ConsumerApi.V1.PingRequest(), 
                    deadline: _dateTimeProvider.UtcNow.AddSeconds(timeout.TotalSeconds), 
                    cancellationToken: cancellationToken)
                .ConfigureAwait(false);
            
            _logger.LogDebug("Successful ping request to Zamza server");

            return true;
        }
        catch (Exception exception)
        {
            _logger.LogDebug(exception, "Ping request to Zamza server resulted with exception");
            return false;
        }
    }

    public async Task Leave(
        LeaveRequest request,
        CancellationToken cancellationToken)
    {
        var leaveTimeout = TimeSpan.FromSeconds(1);
        
        using var scope = _logger.BeginScope(
            "[Leave] ConsumerGroup: {ConsumerGroup}, ConsumerId: {ConsumerId}",
            request.ConsumerGroup, request.ConsumerId);
        
        try
        {
            await _grpcClient
                .LeaveAsync(
                    request.ToGrpc(),
                    deadline: _dateTimeProvider.UtcNow.AddSeconds(leaveTimeout.TotalSeconds),
                    cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.Unavailable)
        {
            _logger.LogError("Zamza server is not available");
            throw new ZamzaException(ZamzaErrorCode.ServerUnavailable);
        }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.InvalidArgument)
        {
            _logger.LogError(exception, "The leave request to Zamza server did not match the protocol");
            throw new ZamzaException(ZamzaErrorCode.InternalError);
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "An unexpected exception occured during leave request");
            throw new ZamzaException(ZamzaErrorCode.InternalError, innerException: exception);
        }
    }

    public void Dispose()
    {
        _grpcChannel.Dispose();
    }

    private static class Log
    {
        public static class Fetch
        {
            public static void Request(ILogger<ZamzaServerFacade<TKey, TValue>> logger, FetchRequest request)
            {
                if (logger.IsEnabled(LogLevel.Debug) is false)
                    return;
                
                logger.LogDebug(
                    "Fetch request to Zamza server for partitions: {FetchedPartitions}",
                    request.FetchedPartitions);
            }

            public static void Result(ILogger<ZamzaServerFacade<TKey, TValue>> logger, FetchResult<TKey, TValue> result)
            {
                if (logger.IsEnabled(LogLevel.Debug) is false)
                    return;

                if (result.IsFetchSuccessful is false)
                {
                    logger.LogDebug("The fetch had irrelevant partition ownerships");
                    return;
                }

                var fetchedMessages = result.Messages
                    .Select(message => (Topic: message.Topic, Partition: message.Partition, Offset: message.Offset))
                    .ToList();
                
                logger.LogDebug("Successful fetch. Messages: {Messages}", fetchedMessages);
            }
        }

        public static class ClaimPartitionOwnership
        {
            public static void Request(
                ILogger<ZamzaServerFacade<TKey, TValue>> logger,
                ClaimPartitionOwnershipRequest request)
            {
                if (logger.IsEnabled(LogLevel.Debug) is false)
                    return;

                var claimedPartition = request.ClaimedPartitions
                    .Select(partition => (Topic: partition.Topic, Partition: partition.Partition))
                    .ToList();
                
                logger.LogDebug(
                    "ClaimPartitionOwnership request. Partitions: {Partitions}",
                    claimedPartition);
            }

            public static void Result(
                ILogger<ZamzaServerFacade<TKey, TValue>> logger,
                ClaimPartitionOwnershipResult result)
            {
                if (logger.IsEnabled(LogLevel.Debug) is false)
                    return;

                logger.LogDebug(result.IsSuccessful ? "Claim successful" : "Claim failed");
            }
        }

        public static class Commit
        {
            public static void Request(ILogger<ZamzaServerFacade<TKey, TValue>> logger, CommitRequest<TKey, TValue> request)
            {
                if (logger.IsEnabled(LogLevel.Debug) is false)
                    return;

                var processedMessages = request.ProcessedMessages
                    .Select(message => (Topic: message.Topic, Partition: message.Partition, Offset: message.Offset));
                var messagesWithRetryableFailure = request.MessagesWithRetryableFailure
                    .Select(message => (Topic: message.Message.Topic, Partition: message.Message.Partition, Offset: message.Message.Offset));
                var messagesWithCompleteFailure = request.MessagesWithCompleteFailure
                    .Select(message => (Topic: message.Message.Topic, Partition: message.Message.Partition, Offset: message.Message.Offset));
                
                logger.LogDebug(
                    "Commit request to Zamza server: \n" +
                    "Processed: {ProcessedMessages}\n Retryable: {RetryableMessages}\n Failed: {FailedMessages}",
                    processedMessages,
                    messagesWithRetryableFailure,
                    messagesWithCompleteFailure);
            }

            public static void Response(ILogger<ZamzaServerFacade<TKey, TValue>> logger, CommitResult result)
            {
                if (logger.IsEnabled(LogLevel.Debug) is false)
                    return;
                
                var partitionsWithIrrelevantOwnerships = result.PartitionsWithIrrelevantOwnership
                    .Select(partition => (Topic: partition.Topic, Partition: partition.Partition));
                logger.LogDebug("Partitions with irrelevant ownerships: {Partitions}", partitionsWithIrrelevantOwnerships);
            }
        }
    }
}