using System.Data;
using System.Data.Common;
using Dapper;
using Microsoft.Data.SqlClient;
using Zamza.Server.Models.Exceptions;
using TimeoutException = Zamza.Server.Models.Exceptions.TimeoutException;

namespace Zamza.Server.DataAccess.Common.QueryExecution;

internal static class SqlExecutionExtensions
{
    private const int TimeoutErrorCode = -2;
    
    public static async Task ExecuteWithExceptionHandling(
        this IDbConnection connection, 
        CommandDefinition command)
    {
        try
        {
            await connection.ExecuteAsync(command);
        }
        catch (Exception exception)
        {
            throw ConvertException(exception);
        }
    }

    public static async Task<IEnumerable<T>> QueryWithExceptionHandling<T>(
        this IDbConnection connection,
        CommandDefinition command)
    {
        try
        {
            return await connection.QueryAsync<T>(command);
        }
        catch (Exception exception)
        {
            throw ConvertException(exception);
        }
    }

    public static async Task<T> QueryFirstWithExceptionHandling<T>(
        this IDbConnection connection,
        CommandDefinition command)
    {
        try
        {
            return await connection.QueryFirstAsync<T>(command);
        }
        catch (Exception exception)
        {
            throw ConvertException(exception);
        }
    }
    
    public static async Task<T?> QueryFirstOrDefaultWithExceptionHandling<T>(
        this IDbConnection connection,
        CommandDefinition command)
    {
        try
        {
            return await connection.QueryFirstOrDefaultAsync<T>(command);
        }
        catch (Exception exception)
        {
            throw ConvertException(exception);
        }
    }

    public static IAsyncEnumerable<T> QueryUnbufferedWithExceptionHandling<T>(
        this DbConnection connection,
        string sql,
        object? parameters,
        int timeout)
    {
        try
        {
            return connection.QueryUnbufferedAsync<T>(
                sql: sql,
                param: parameters,
                commandTimeout: timeout);
        }
        catch (Exception exception)
        {
            throw ConvertException(exception);
        }
    }
    
    private static Exception ConvertException(Exception exception)
    {
        if (exception is SqlException {Number: TimeoutErrorCode})
        {
            return new TimeoutException("The query to database has timed out");
        }
        
        return new InternalException("An error occured while executing the query to database", exception);
    }
}