using Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;
using Zamza.Server.DataAccess.Repositories.CommonModels;

namespace Zamza.Server.DataAccess.Repositories.DLQRepository;

public interface IDLQRepository
{
    Task DeleteMessages(
        IDbTransactionFrame transaction,
        string consumerGroup,
        IReadOnlyCollection<MessageToDelete> messages,
        CancellationToken cancellationToken);
}