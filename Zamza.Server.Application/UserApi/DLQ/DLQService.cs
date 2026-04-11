using Zamza.Server.Application.UserApi.DLQ.Models;
using Zamza.Server.DataAccess.Repositories.DLQRepository;

namespace Zamza.Server.Application.UserApi.DLQ;

internal sealed class DLQService : IDLQService
{
    private const int FirstPageStartId = -1;
    
    private readonly IDLQRepository _dlqRepository;

    public DLQService(IDLQRepository dlqRepository)
    {
        _dlqRepository = dlqRepository;
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
}