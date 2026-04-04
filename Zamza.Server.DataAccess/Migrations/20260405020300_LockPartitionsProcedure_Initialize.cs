using FluentMigrator;

namespace Zamza.Server.DataAccess.Migrations;

[Migration(20260405020300)]
public sealed class _20260405020300_LockPartitionsProcedure_Initialize : SqlMigrationBase
{
    protected override string SqlUp => 
    """
        create or replace procedure zamza.lock_partitions(
            consumer_group text,
            topics text[],
            partitions int[]
        )
        language plpgsql
        as
        $$
            declare
                index int;
            begin
                for index in
                    select idx from generate_subscripts(topics, 1) as idx
                    order by topics[idx], partitions[idx]
                loop
                    perform pg_advisory_xact_lock(
                        hashtextextended(consumer_group || ';' || topics[index] || ';' || partitions[index]::text, 0)
                    );
                end loop;
            end;
        $$;
    """;
    
    protected override string SqlDown => 
    """
        drop procedure if exists zamza.lock_partitions(text, text[], int[]);                                     
    """;
}