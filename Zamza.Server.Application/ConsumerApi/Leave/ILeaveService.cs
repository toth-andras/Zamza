using Zamza.Server.Application.ConsumerApi.Leave.Models;

namespace Zamza.Server.Application.ConsumerApi.Leave;

public interface ILeaveService
{
    Task Leave(LeaveRequest request, CancellationToken cancellationToken);
}