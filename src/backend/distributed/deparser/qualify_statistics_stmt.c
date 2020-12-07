/*-------------------------------------------------------------------------
 *
 * qualify_statistics_stmt.c
 *	  Functions specialized in fully qualifying all statistics statements.
 *    These functions are dispatched from qualify.c
 *
 *	  Goal would be that the deparser functions for these statements can
 *	  serialize the statement without any external lookups.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "distributed/deparser.h"
#include "nodes/parsenodes.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"

void
QualifyCreateStatisticsStmt(Node *node)
{
	CreateStatsStmt *stmt = castNode(CreateStatsStmt, node);

	RangeVar *relation = (RangeVar *) linitial(stmt->relations);

	if (relation->schemaname == NULL)
	{
		Oid tableOid = RelnameGetRelid(relation->relname);
		Oid schemaOid = get_rel_namespace(tableOid);
		relation->schemaname = get_namespace_name(schemaOid);
	}
}