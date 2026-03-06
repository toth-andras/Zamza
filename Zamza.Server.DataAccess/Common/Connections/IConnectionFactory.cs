using System.Data.Common;

namespace Zamza.Server.DataAccess.Common.Connections;

public interface IConnectionFactory
{
    Task<DbConnection> CreateConnection(CancellationToken cancellation);
}