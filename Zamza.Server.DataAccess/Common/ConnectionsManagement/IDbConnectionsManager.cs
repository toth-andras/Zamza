using System.Data;
using System.Data.Common;
using Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;

namespace Zamza.Server.DataAccess.Common.ConnectionsManagement;

public interface IDbConnectionsManager
{
    Task<DbConnection> CreateConnection(CancellationToken cancellation);

    Task<IDbTransactionFrame> BeginTransaction(
        IsolationLevel isolationLevel,
        CancellationToken cancellation);
}