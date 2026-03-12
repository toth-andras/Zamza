using Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;
using Zamza.Server.Models.ConsumerApi;

namespace Zamza.Server.DataAccess.Repositories.DlqRepository;

public interface IDlqRepository
{
    Task ClearMessages(
        IDbTransactionFrame transaction,
        MessageKeysSet messagesToRemove,
        CancellationToken cancellationToken);

    Task Insert(
        IDbTransactionFrame transaction,
        string consumerGroup,
        IReadOnlyList<ConsumerApiMessage> messages,
        CancellationToken cancellationToken);
}