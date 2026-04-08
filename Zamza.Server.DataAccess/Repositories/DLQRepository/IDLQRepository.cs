using Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;
using Zamza.Server.DataAccess.Repositories.CommonModels;
using Zamza.Server.Models.ConsumerApi.Commit;
using Zamza.Server.Models.UserApi;

namespace Zamza.Server.DataAccess.Repositories.DLQRepository;

public interface IDLQRepository
{
    Task Delete(
        IDbTransactionFrame transaction,
        MessageToDelete message,
        CancellationToken cancellationToken);
    
    Task Delete(
        IDbTransactionFrame transaction,
        string consumerGroup,
        IReadOnlyCollection<MessageToDelete> messages,
        CancellationToken cancellationToken);

    Task Upsert(
        IDbTransactionFrame transaction,
        string consumerGroup,
        IReadOnlyCollection<FailedMessage> messages,
        CancellationToken cancellationToken);

    Task<List<UserApiDLQMessage>> Get(
        long startId,
        int limit,
        CancellationToken cancellationToken);
}