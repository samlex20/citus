-- citus--10.0-1--9.5-1
-- this is an empty downgrade path since citus--9.5-1--10.0-1.sql is empty for now

#include "../udfs/citus_finish_pg_upgrade/9.5-1.sql"

#include "../../../columnar/sql/downgrades/columnar--10.0-1--9.5-1.sql"

DROP VIEW public.citus_tables;

DROP FUNCTION pg_catalog.alter_distributed_table(table_name regclass, distribution_column text DEFAULT NULL, shard_count int default NULL, colocate_with text DEFAULT 'default', cascade_to_colocated boolean DEFAULT NULL);
DROP FUNCTION pg_catalog.alter_table_set_access_method(table_name regclass, access_method text);
