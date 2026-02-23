using FluentMigrator;

namespace Zamza.Server.DataAccess.Migrations;

[Migration(20260223235900)]
public sealed class _20260223235900_Schema_Initialize : Migration
{
    private const string SchemaName = "zamza";
    
    public override void Up()
    {
        Execute.Sql($"create schema if not exists {SchemaName};");
    }

    public override void Down()
    {
        Execute.Sql($"drop schema if exists {SchemaName};");
    }
}