using System.Data.Common;

namespace Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;

internal sealed class DbTransactionFrame : IDbTransactionFrame
{
    public DbConnection Connection { get; }
    public DbTransaction Transaction { get; }

    public DbTransactionFrame(DbConnection connection, DbTransaction transaction)
    {
        Connection = connection;
        Transaction = transaction;
    }
    
    public async ValueTask Commit(CancellationToken cancellation)
    {
        await Transaction.CommitAsync(cancellation);
    }

    public async ValueTask Rollback(CancellationToken cancellation)
    {
        await Transaction.RollbackAsync(cancellation);
    }
    
    public async ValueTask DisposeAsync()
    {
        await TransactionAndConnectionDisposer.DisposeAsync(Transaction, Connection);
    }
}