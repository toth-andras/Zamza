using System.Text.Json;
using Microsoft.Extensions.Logging;
using Zamza.Server.Application.UserApi.DLQ.Models;
using Zamza.Server.DataAccess.Repositories.CommonModels;
using Zamza.Server.DataAccess.Repositories.DLQRepository;

namespace Zamza.Server.Application.UserApi.DLQ;

internal sealed class DLQService : IDLQService
{
    private const int FirstPageStartId = -1;
    
    private readonly IDLQRepository _dlqRepository;
    private readonly ILogger<DLQService> _logger;

    public DLQService(
        IDLQRepository dlqRepository,
        ILogger<DLQService> logger)
    {
        _dlqRepository = dlqRepository;
        _logger = logger;
    }

    public async Task<GetDLQMessagesResponse> GetMessages(
        GetDLQMessagesRequest request,
        CancellationToken cancellationToken)
    {
        var startId = request.Cursor ?? FirstPageStartId;

        var messages = await _dlqRepository.Get(
            startId,
            request.Limit,
            cancellationToken);

        var newCursor = messages.Count > 0 
            ? messages.Max(message => message.Id)
            : startId;
        
        return new GetDLQMessagesResponse(
            messages,
            newCursor);
    }

    public async Task DeleteMessages(
        DeleteDLQMessagesRequest request,
        CancellationToken cancellationToken)
    {
        if (request.Messages.Count == 0)
        {
            return;
        }

        var messagesToDelete = request.Messages
            .Select(message => new MessageToDelete(message.Topic, message.Partition, message.Offset))
            .ToList();

        await _dlqRepository.Delete(
            request.ConsumerGroup,
            messagesToDelete,
            cancellationToken);
        
        LogDeletedMessages(request);
    }

    private void LogDeletedMessages(DeleteDLQMessagesRequest request)
    {
        var messagesJson = JsonSerializer.Serialize(request.Messages);
        
        _logger.LogInformation(
            "For consumer group \'{ConsumerGroup}\', the following messages have been deleted: {Messages}",
            request.ConsumerGroup,
            messagesJson);
    }
}