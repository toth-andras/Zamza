using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using Zamza.Consumer.Internal.Utils.DateTimeProvider;
using Zamza.Consumer.Internal.ZamzaServer.Exceptions;
using Zamza.Consumer.Internal.ZamzaServer.Mapping;
using Zamza.Consumer.Internal.ZamzaServer.Models;
using Zamza.ConsumerApi.V1;
using FetchRequest = Zamza.Consumer.Internal.ZamzaServer.Models.FetchRequest;
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

    public async Task<FetchResult<TKey, TValue>> Fetch(
        FetchRequest request,
        CancellationToken cancellationToken)
    {
        var fetchTimeout = TimeSpan.FromSeconds(1);

        using var scope = _logger.BeginScope(
            "ConsumerGroup: {ConsumerGroup}, ConsumerId: {ConsumerId}",
            request.ConsumerGroup, request.ConsumerId);

        FetchResponse grpcResponse;
        try
        {
            Log.Fetch.Request(_logger, request);
            
            grpcResponse = await _grpcClient.FetchAsync(
                request.ToGrpc(),
                deadline: _dateTimeProvider.UtcNow.AddSeconds(fetchTimeout.TotalSeconds),
                cancellationToken: cancellationToken);
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
            throw new ZamzaException(ZamzaErrorCode.InternalError);
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
    
    public async Task<bool> Ping(
        PingRequest request,
        CancellationToken cancellationToken)
    {
        using var scope = _logger.BeginScope(
            "ConsumerGroup: {ConsumerGroup}, ConsumerId: {ConsumerId}",
            request.ConsumerGroup, request.ConsumerId);
        
        _logger.LogDebug("Ping request to Zamza server");
        
        var timeout = TimeSpan.FromSeconds(1);
        try
        {
            await _grpcClient.PingAsync(
                new ConsumerApi.V1.PingRequest(),
                deadline: _dateTimeProvider.UtcNow.AddSeconds(timeout.TotalSeconds),
                cancellationToken: cancellationToken);
            
            _logger.LogDebug("Successful ping request to Zamza server");

            return true;
        }
        catch (Exception exception)
        {
            _logger.LogDebug(exception, "Ping request to Zamza server resulted with exception");
            return false;
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
    }
}