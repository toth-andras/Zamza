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
            throw new ZamzaException(ZamzaErrorCode.ServerUnavailable);
        }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.InvalidArgument)
        {
            _logger.LogTrace(
                exception, 
                "The fetch request to Zamza server did not match the protocol: {ErrorMessage}",
                exception.Message);
            
            throw new ZamzaException(ZamzaErrorCode.InternalError);
        }
        catch (Exception exception)
        {
            _logger.LogTrace(
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
        
        return result;
    }

    public async Task<FetchResult<TKey, TValue>> Fetch(
        FetchRequest request,
        CancellationToken cancellationToken)
    {
        var fetchTimeout = TimeSpan.FromSeconds(1);

        FetchResponse grpcResponse;
        try
        {
            grpcResponse = await _grpcClient
                .FetchAsync(
                    request.ToGrpc(), 
                    deadline: _dateTimeProvider.UtcNow.AddSeconds(fetchTimeout.TotalSeconds), 
                    cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.Unavailable)
        {
            throw new ZamzaException(ZamzaErrorCode.ServerUnavailable);
        }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.InvalidArgument)
        {
            _logger.LogTrace(
                exception, 
                "The fetch request to Zamza server did not match the protocol: {ErrorMessage}",
                exception.Message);
            throw new ZamzaException(ZamzaErrorCode.InternalError);
        }
        catch (Exception exception)
        {
            _logger.LogTrace(
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
            
            return result;
        }

        // Should not get here
        throw new NotSupportedException("The version of protocol used by Zamza server is not supported");
    }

    public async Task<CommitResult> Commit(
        CommitRequest<TKey, TValue> request,
        CancellationToken cancellationToken)
    {
        var commitTimeout = TimeSpan.FromSeconds(3);
        try
        {
            var grpcResult= await _grpcClient
                .CommitAsync(request.ToGrpc(), 
                    deadline: _dateTimeProvider.UtcNow.AddSeconds(commitTimeout.TotalSeconds), 
                    cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            var result = grpcResult.ToModel();

            return result;
        }
        catch (RpcException exception) when (exception.StatusCode is StatusCode.Unavailable)
        {
            throw new ZamzaException(ZamzaErrorCode.ServerUnavailable);
        }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.InvalidArgument)
        {
            _logger.LogTrace(exception, "Commit request did not match the protocol");
            throw new ZamzaException(ZamzaErrorCode.InternalError, innerException: exception);
        }
        catch (Exception exception)
        {
            _logger.LogTrace(exception, "An unexpected exception occured during Commit");
            throw new ZamzaException(ZamzaErrorCode.InternalError);
        }
    }

    public async Task<bool> Ping(
        PingRequest request,
        CancellationToken cancellationToken)
    {
        var timeout = TimeSpan.FromSeconds(1);
        try
        {
            await _grpcClient
                .PingAsync(
                    new ConsumerApi.V1.PingRequest(), 
                    deadline: _dateTimeProvider.UtcNow.AddSeconds(timeout.TotalSeconds), 
                    cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            return true;
        }
        catch (Exception exception)
        {
            _logger.LogTrace(exception, "Ping request to Zamza server resulted with exception");
            return false;
        }
    }

    public async Task Leave(
        LeaveRequest request,
        CancellationToken cancellationToken)
    {
        var leaveTimeout = TimeSpan.FromSeconds(1);
        
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
            throw new ZamzaException(ZamzaErrorCode.ServerUnavailable);
        }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.InvalidArgument)
        {
            _logger.LogTrace(exception, "The leave request to Zamza server did not match the protocol");
            throw new ZamzaException(ZamzaErrorCode.InternalError);
        }
        catch (Exception exception)
        {
            _logger.LogTrace(exception, "An unexpected exception occured during leave request");
            throw new ZamzaException(ZamzaErrorCode.InternalError, innerException: exception);
        }
    }

    public void Dispose()
    {
        _grpcChannel.Dispose();
    }
}