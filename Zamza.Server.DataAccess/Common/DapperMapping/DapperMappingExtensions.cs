using Dapper;

namespace Zamza.Server.DataAccess.Common.DapperMapping;

public static class DapperMappingExtensions
{
    public static void Configure()
    {
        DefaultTypeMap.MatchNamesWithUnderscores = true;
        SqlMapper.AddTypeHandler(new JsonBTypeMapper<Dictionary<string, byte[]>>());
    }
}