using System.Data;
using System.Text.Json;
using Dapper;
using Npgsql;
using NpgsqlTypes;

namespace Zamza.Server.DataAccess.Common.DapperMapping;

public sealed class JsonBTypeMapper<T> : SqlMapper.TypeHandler<T> where T : class
{
    public override T? Parse(object? value)
    {
        if (value is null || value == DBNull.Value)
        {
            return null;
        }

        var raw = value.ToString();
        
        return raw is not null 
            ? JsonSerializer.Deserialize<T>(raw)
            : null;
    }

    public override void SetValue(IDbDataParameter parameter, T? value)
    {
        parameter.Value = value is null
            ? string.Empty
            : JsonSerializer.Serialize(value);

        if (parameter is NpgsqlParameter npgsqlParameter)
        {
            npgsqlParameter.NpgsqlDbType = NpgsqlDbType.Jsonb;
        }
    }
}