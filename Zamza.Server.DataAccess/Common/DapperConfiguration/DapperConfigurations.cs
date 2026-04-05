using Dapper;

namespace Zamza.Server.DataAccess.Common.DapperConfiguration;

internal static class DapperConfigurations
{
    public static void Configure()
    {
        SqlMapper.Settings.CommandTimeout = 60;
        
        DefaultTypeMap.MatchNamesWithUnderscores = true;
        SqlMapper.AddTypeHandler(new JsonBTypeMapper<Dictionary<string, byte[]>>());
    }
}