using Zamza.Consumer.Internal.ZamzaServer.Models;

namespace Zamza.Consumer.Internal.ZamzaServer;

internal interface IZamzaServerFacade<TKey, TValue>
{
    Task<ClaimPartitionOwnershipResult> ClaimPartitionOwnership(
        ClaimPartitionOwnershipRequest request,
        CancellationToken cancellationToken);
    
    Task<FetchResult<TKey, TValue>> Fetch(
        FetchRequest request,
        CancellationToken cancellationToken);
    
    Task<bool> Ping(
        PingRequest request,
        CancellationToken cancellationToken);
    
    Task Leave(
        LeaveRequest request,
        CancellationToken cancellationToken);
}