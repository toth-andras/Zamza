using FluentMigrator;

namespace Zamza.Server.DataAccess.Migrations;

[Migration(20260306130500)]
public sealed class _20260306130500_InitialMigration : SqlMigrationBase
{
    protected override string SqlUp => "select 0;";
    protected override string SqlDown => "select 0;";
}