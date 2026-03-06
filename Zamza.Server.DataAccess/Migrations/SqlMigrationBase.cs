using FluentMigrator;

namespace Zamza.Server.DataAccess.Migrations;

public abstract class SqlMigrationBase : Migration
{
    protected abstract string SqlUp { get; }
    protected abstract string SqlDown { get; }

    public override void Up()
    {
        Execute.Sql(SqlUp);
    }

    public override void Down()
    {
        Execute.Sql(SqlDown);
    }
}