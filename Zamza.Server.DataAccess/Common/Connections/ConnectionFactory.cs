using System.Data;
using System.Data.Common;

namespace Zamza.Server.DataAccess.Common.Connections;

internal sealed class ConnectionFactory : IConnectionFactory
{
    private readonly DbDataSource _dataSource;

    public ConnectionFactory(DbDataSource dataSource)
    {
        _dataSource = dataSource;
    }
    
    public async Task<DbConnection> CreateConnection(CancellationToken cancellation)
    {
        var connection = _dataSource.CreateConnection();
        await connection.OpenAsync(cancellation);

        if (connection.State is not ConnectionState.Open)
        {
            throw new ApplicationException("Could not open db connection");
        }

        return connection;
    }
}