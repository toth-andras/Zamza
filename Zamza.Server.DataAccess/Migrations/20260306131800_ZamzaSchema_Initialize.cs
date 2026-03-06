using FluentMigrator;

namespace Zamza.Server.DataAccess.Migrations;

[Migration(20260306131800)]
public sealed class _20260306131800_ZamzaSchema_Initialize : SqlMigrationBase
{
    private const string SchemaName = "zamza";
    
    protected override string SqlUp => $"create schema if not exists {SchemaName};";
    protected override string SqlDown => $"drop schema if exists {SchemaName} cascade;";
}