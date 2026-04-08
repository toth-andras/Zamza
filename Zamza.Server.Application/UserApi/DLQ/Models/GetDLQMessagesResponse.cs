using Zamza.Server.Models.UserApi;

namespace Zamza.Server.Application.UserApi.DLQ.Models;

public sealed record GetDLQMessagesResponse(
    IReadOnlyCollection<UserApiDLQMessage> Messages,
    long Cursor);