-- 
--  Failure tests for real time select queries
-- 
CREATE SCHEMA real_time_select_failure;
SET search_path TO 'real_time_select_failure';
SET citus.next_shard_id TO 190000;

-- Preparation
SELECT citus.mitmproxy('conn.allow()');
SET citus.shard_count to 4;
SET citus.shard_replication_factor to 1;

-- create tables
CREATE TABLE test_table(id int, value_1 int, value_2 int);
SELECT create_distributed_table('test_table','id');

-- Populate data to the table
INSERT INTO test_table VALUES(1,1,1),(1,2,2),(2,1,1),(2,2,2),(3,1,1),(3,2,2);

-- Create a function to make sure that queries returning the same result 
CREATE FUNCTION raise_failed_execution(query text) RETURNS void AS $$
BEGIN
	EXECUTE query;
	EXCEPTION WHEN OTHERS THEN
	IF SQLERRM LIKE 'failed to execute task%' THEN
		RAISE 'Task failed to execute';
	END IF;
END;
$$LANGUAGE plpgsql;

-- Kill when the first COPY command arrived, since we have a single placement 
-- it is expected to error out.
SET client_min_messages TO ERROR;
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").kill()');
SELECT raise_failed_execution('SELECT count(*) FROM test_table');
SET client_min_messages TO DEFAULT;

-- Kill the connection with a CTE
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").kill()');
WITH
results AS (SELECT * FROM test_table)
SELECT * FROM test_table, results
WHERE test_table.id = results.id;

-- Since the outer query uses the connection opened by the CTE,
-- killing connection after first successful query should break.
SET client_min_messages TO ERROR;
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").after(1).kill()');
SELECT raise_failed_execution('WITH
			results AS (SELECT * FROM test_table)
			SELECT * FROM test_table, results
			WHERE test_table.id = results.id');
SET client_min_messages TO DEFAULT;

-- In parallel execution mode Citus opens separate connections for each shard
-- so killing the connection after the first copy does not break it.
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").after(1).kill()');
SELECT count(*) FROM test_table;

-- Cancel a real-time executor query
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").cancel(' || pg_backend_pid() || ')');
SELECT count(*) FROM test_table;

-- Cancel a query within the transaction
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").cancel(' || pg_backend_pid() || ')');
BEGIN;
SELECT count(*) FROM test_table;
COMMIT;

-- Cancel a query within the transaction after a multi-shard update
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").cancel(' || pg_backend_pid() || ')');
BEGIN;
UPDATE test_table SET value_1 = value_1 + 1;
SELECT count(*) FROM test_table;
COMMIT;

-- Cancel a query with CTE
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").cancel(' || pg_backend_pid() || ')');
WITH
results AS (SELECT * FROM test_table)
SELECT * FROM test_table
WHERE test_table.id > (SELECT id FROM results);

-- Since Citus opens a new connection after a failure within the real time
-- execution and after(1).kill() kills connection after a successful execution
-- for each connection, following transaciton does not fail.
SET citus.multi_shard_modify_mode to sequential;
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").after(1).kill()');
BEGIN;
SELECT count(*) FROM test_table;
COMMIT;

-- Cancel a real-time executor query - in sequential mode
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").cancel(' || pg_backend_pid() || ')');
SELECT count(*) FROM test_table;

-- Cancel a query within the transaction - in sequential mode
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").cancel(' || pg_backend_pid() || ')');
BEGIN;
SELECT count(*) FROM test_table;
COMMIT;

-- Cancel the query within a transaction after a single succesful run
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").after(1).cancel(' || pg_backend_pid() || ')');
BEGIN;
SELECT count(*) FROM test_table;
COMMIT;

-- Now, test with replication factor 2, tests are expected to pass
-- since we have two placements for the same shard
DROP TABLE test_table;
SET citus.multi_shard_modify_mode to default;

-- Create table with shard placements on each node
SET citus.shard_replication_factor to 2;
CREATE TABLE test_table(id int, value_1 int, value_2 int);
SELECT create_distributed_table('test_table','id');

-- Populate data to the table
INSERT INTO test_table VALUES(1,1,1),(1,2,2),(2,1,1),(2,2,2),(3,1,1),(3,2,2);

-- Kill when the first COPY command arrived, since we have placements on each node
-- it shouldn't fail.
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").kill()');
SELECT count(*) FROM test_table;

-- Kill within the transaction, since we have placements on each node
-- it shouldn't fail.
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").kill()');
BEGIN;
SELECT count(*) FROM test_table;
COMMIT;

-- Cancel a real-time executor query
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").cancel(' || pg_backend_pid() || ')');
SELECT count(*) FROM test_table;

-- Cancel a query within the transaction
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").cancel(' || pg_backend_pid() || ')');
BEGIN;
SELECT count(*) FROM test_table;
COMMIT;

-- Cancel a query within the transaction after a multi-shard update
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").cancel(' || pg_backend_pid() || ')');
BEGIN;
UPDATE test_table SET value_1 = value_1 + 1;
SELECT count(*) FROM test_table;
COMMIT;

-- Cancel a query with CTE
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").cancel(' || pg_backend_pid() || ')');
WITH
results AS (SELECT * FROM test_table)
SELECT * FROM test_table
WHERE test_table.id > (SELECT id FROM results);

-- Since we have the placement on each node, test with sequential mode
-- should pass as well.
SET citus.multi_shard_modify_mode to sequential;
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").after(1).kill()');
BEGIN;
SELECT count(*) FROM test_table;
COMMIT;

DROP SCHEMA real_time_select_failure CASCADE;
SET search_path TO default;