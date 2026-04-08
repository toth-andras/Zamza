using Zamza.Server.Application.UserApi.DLQ.Models;
using Zamza.Server.UserApi.Controllers.V1.DLQ.Models;

namespace Zamza.Server.UserApi.Controllers.V1.DLQ.Mapping;

internal static class GetDLQMessagesMappingExtensions
{
    public static GetDLQMessagesRestResponse ToRest(this GetDLQMessagesResponse response)
    {
        return new GetDLQMessagesRestResponse
        {
            Messages = response.Messages
                .Select(message => message.ToRest())
                .ToArray(),
            Cursor = response.Cursor
        };
    }
}