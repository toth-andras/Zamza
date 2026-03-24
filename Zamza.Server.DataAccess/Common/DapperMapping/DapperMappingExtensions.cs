using Dapper;

namespace Zamza.Server.DataAccess.Common.DapperMapping;

internal static class DapperMappingExtensions
{
    public static void Configure()
    {
        DefaultTypeMap.MatchNamesWithUnderscores = true;
        SqlMapper.AddTypeHandler(new JsonBTypeMapper<Dictionary<string, byte[]>>());
    }
}