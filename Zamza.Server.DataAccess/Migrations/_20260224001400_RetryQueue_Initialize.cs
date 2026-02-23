using FluentMigrator;

namespace Zamza.Server.DataAccess.Migrations;

[Migration(20260224001400)]
public sealed class _20260224001400_RetryQueue_Initialize : Migration
{
    public override void Up()
    {
        var sql =
        """
            create table if not exists zamza.retry_queue
            (
                version smallint not null,
                consumer_group_name varchar not null,
                topic varchar not null,
                partition_num varchar not null,
                offset_num int not null,
                message_headers bytea,
                message_key bytea,
                message_value bytea,
                message_timestamp timestamp with time zone not null,
                max_retries int not null,
                min_retries_gap_ms bigint not null,
                processing_deadline timestamp with time zone not null,
                next_retry_after timestamp with time zone not null,
                retries_count int not null,
                retry_reason jsonb
            );                                   
        """;
        
        Execute.Sql(sql);
    }

    public override void Down()
    {
        Execute.Sql("drop table if exists zamza.retry_queue");
    }
}