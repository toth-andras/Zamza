using System.Data.Common;

namespace Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;

internal static class TransactionAndConnectionDisposer
{
    public static async ValueTask DisposeAsync(
        DbTransaction? transaction,
        DbConnection? connection)
    {
        try
        {
            if (transaction is not null)
            {
                await transaction.DisposeAsync();
            }
        }
        finally
        {
            if (connection is not null)
            {
                await connection.DisposeAsync();
            }
        }
    }
}