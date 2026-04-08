using Zamza.Server.Models.Validators;

namespace Zamza.Server.Application.UserApi.DLQ.Models;

public sealed record GetDLQMessagesRequest
{
    public int Limit { get; }
    public long? Cursor { get; }

    public GetDLQMessagesRequest(int limit, long? cursor)
    {
        ThrowBadRequest.IfNegative(limit, "limit");
        
        Limit = limit;
        Cursor = cursor;
    }
}