using System.Data;
using System.Data.Common;
using Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;
using Zamza.Server.Models.Exceptions;

namespace Zamza.Server.DataAccess.Common.ConnectionsManagement;

internal sealed class DbConnectionsManager : IDbConnectionsManager
{
    private readonly DbDataSource _dataSource;

    public DbConnectionsManager(DbDataSource dataSource)
    {
        _dataSource = dataSource;
    }
    
    public async Task<DbConnection> CreateConnection(CancellationToken cancellation)
    {
        var connection = _dataSource.CreateConnection();
        await connection.OpenAsync(cancellation);

        if (connection.State is not ConnectionState.Open)
        {
            throw new InternalException("Could not open db connection");
        }

        return connection;
    }

    public async Task<IDbTransactionFrame> BeginTransaction(
        IsolationLevel isolationLevel,
        CancellationToken cancellationToken)
    {
        DbConnection? connection = null;
        DbTransaction? transaction = null;
        try
        {
            connection = await CreateConnection(cancellationToken);
            transaction = await connection.BeginTransactionAsync(isolationLevel, cancellationToken);
        }
        catch (Exception)
        {
            await TransactionAndConnectionDisposer.DisposeAsync(transaction, connection);
        }

        if (connection is null || transaction is null)
        {
            throw new InternalException("Could not begin transaction");
        }
        
        return new DbTransactionFrame(connection, transaction);
    }
}