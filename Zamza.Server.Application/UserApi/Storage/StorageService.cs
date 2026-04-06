using System.Data;
using Microsoft.Extensions.Logging;
using Zamza.Server.Application.UserApi.Storage.Models;
using Zamza.Server.DataAccess.Common.ConnectionsManagement;
using Zamza.Server.DataAccess.Repositories.CommonModels;
using Zamza.Server.DataAccess.Repositories.DLQRepository;
using Zamza.Server.DataAccess.Repositories.RetryQueueRepository;

namespace Zamza.Server.Application.UserApi.Storage;

internal sealed class StorageService : IStorageService
{
    private readonly IDbConnectionsManager _connectionsManager;
    private readonly IRetryQueueRepository _retryQueueRepository;
    private readonly IDLQRepository _dlqRepository;
    private readonly ILogger<StorageService> _logger;

    public StorageService(
        IDbConnectionsManager connectionsManager,
        IRetryQueueRepository retryQueueRepository,
        IDLQRepository dlqRepository,
        ILogger<StorageService> logger)
    {
        _connectionsManager = connectionsManager;
        _retryQueueRepository = retryQueueRepository;
        _dlqRepository = dlqRepository;
        _logger = logger;
    }

    public async Task DeleteMessage(
        DeleteMessageRequest request,
        CancellationToken cancellationToken)
    {
        await using var transaction = await _connectionsManager.BeginTransaction(
            IsolationLevel.ReadCommitted,
            cancellationToken);

        var messageToDelete = new MessageToDelete(
            request.Topic,
            request.Partition,
            request.Offset);
        
        await _retryQueueRepository.Delete(
            transaction,
            messageToDelete,
            cancellationToken);

        await _dlqRepository.Delete(
            transaction,
            messageToDelete,
            cancellationToken);

        await transaction.Commit(cancellationToken);
        
        _logger.LogInformation(
            "Message (Topic = \'{Topic}\', Partition = {Partition}, Offset = {Offset}) has been deleted from Zamza",
            messageToDelete.Topic,
            messageToDelete.Partition,
            messageToDelete.Offset);
    }
}