using Zamza.Server.Application.UserApi.DLQ.Models;
using Zamza.Server.UserApi.Controllers.V1.DLQ.Models;

namespace Zamza.Server.UserApi.Controllers.V1.DLQ.Mapping;

internal static class DeleteDLQMessagesMappingExtensions
{
    public static DeleteDLQMessagesRequest ToModel(this DeleteDLQMessagesRestRequest request)
    {
        return new DeleteDLQMessagesRequest(
            request.ConsumerGroup,
            request.Messages
                .Select(message => message.ToModel())
                .ToList());
    }

    private static DLQMessageToDelete ToModel(this DLQMessageToDeleteDto message)
    {
        return new DLQMessageToDelete(
            message.Topic,
            message.Partition,
            message.Offset);
    }
}