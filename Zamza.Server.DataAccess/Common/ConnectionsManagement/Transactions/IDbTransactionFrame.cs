using System.Data.Common;

namespace Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;

public interface IDbTransactionFrame : IAsyncDisposable
{
    DbConnection Connection { get; }
    DbTransaction Transaction { get; }
    
    ValueTask Commit(CancellationToken cancellation);
    ValueTask Rollback(CancellationToken cancellation);
}