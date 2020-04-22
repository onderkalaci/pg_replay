
#include "postgres.h"
#include "miscadmin.h"

#include "executor/execdesc.h"
#include "executor/executor.h"
#include "utils/elog.h"
#include "utils/palloc.h"
#include "utils/builtins.h"
#include "libpq/pqformat.h"
#include "nodes/makefuncs.h"
#include "optimizer/planner.h"
#include "optimizer/optimizer.h"
#include "tcop/utility.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif


static int OperationLevel = 0;

void _PG_init(void);
static PlannedStmt * pg_replay_planner(Query *parse, int cursorOptions, ParamListInfo
									   boundParams);
static void pg_replay_ProcessUtility(PlannedStmt *pstmt,
									 const char *queryString,
									 ProcessUtilityContext context,
									 ParamListInfo params,
									 struct QueryEnvironment *queryEnv,
									 DestReceiver *dest,
									 char *completionTag);
static void PgReplayExecutorEnd(QueryDesc *queryDesc);
static void PgReplayExecutorStart(QueryDesc *queryDesc, int eflags);
static void PgReplayExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64
								count,
								bool execute_once);

static planner_hook_type prev_planner_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;


Datum add_replay_target(PG_FUNCTION_ARGS);
Datum remove_replay_target(PG_FUNCTION_ARGS);


PG_FUNCTION_INFO_V1(add_replay_target);
PG_FUNCTION_INFO_V1(remove_replay_target);

void
_PG_init(void)
{
	/* intercept planner */
	prev_planner_hook = planner_hook;
	planner_hook = pg_replay_planner;

	/* register utility hook */
	ProcessUtility_hook = pg_replay_ProcessUtility;

	/* register for executor hook */
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = PgReplayExecutorStart;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = PgReplayExecutorEnd;

	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = PgReplayExecutorRun;
}


static void
PgReplayExecutorStart(QueryDesc *queryDesc, int eflags)
{
	OperationLevel++;

	PG_TRY();
	{
		if (prev_ExecutorStart)
		{
			prev_ExecutorStart(queryDesc, eflags);
		}
		else
		{
			standard_ExecutorStart(queryDesc, eflags);
		}
	}
	PG_CATCH();
	{
		OperationLevel--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}


static void
PgReplayExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count,
					bool execute_once)
{
	PG_TRY();
	{
		if (prev_ExecutorRun)
		{
			prev_ExecutorRun(queryDesc, direction, count, execute_once);
		}
		else
		{
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
		}
	}
	PG_CATCH();
	{
		OperationLevel--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}


static void
PgReplayExecutorEnd(QueryDesc *queryDesc)
{
	PG_TRY();
	{
		if (prev_ExecutorEnd)
		{
			prev_ExecutorEnd(queryDesc);
		}
		else
		{
			standard_ExecutorEnd(queryDesc);
		}
	}
	PG_CATCH();
	{
		OperationLevel--;
		PG_RE_THROW();
	}
	PG_END_TRY();
	OperationLevel--;
}


static void
pg_replay_ProcessUtility(PlannedStmt *pstmt,
						 const char *queryString,
						 ProcessUtilityContext context,
						 ParamListInfo params,
						 struct QueryEnvironment *queryEnv,
						 DestReceiver *dest,
						 char *completionTag)
{
	if (OperationLevel == 0)
	{
		elog(INFO, "pg_replay_ProcessUtility: %s", queryString);
	}


	OperationLevel++;

	PG_TRY();
	{
		standard_ProcessUtility(pstmt, queryString, context,
								params, queryEnv, dest, completionTag);
	}
	PG_CATCH();
	{
		OperationLevel--;
		PG_RE_THROW();
	}
	PG_END_TRY();

	OperationLevel--;
}


static PlannedStmt *
pg_replay_planner(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *result = NULL;

	if (OperationLevel == 0)
	{
		elog(INFO, "planner is called");
	}

	++OperationLevel;

	PG_TRY();
	{
		if (prev_planner_hook)
		{
			result = prev_planner_hook(parse, cursorOptions,
									   boundParams);
		}
		else
		{
			result = standard_planner(parse, cursorOptions,
									  boundParams);
		}
	}
	PG_CATCH();
	{
		OperationLevel--;
		PG_RE_THROW();
	}
	PG_END_TRY();

	OperationLevel--;
	return result;
}


Datum
add_replay_target(PG_FUNCTION_ARGS)
{
	PG_RETURN_VOID();
}


Datum
remove_replay_target(PG_FUNCTION_ARGS)
{
	PG_RETURN_VOID();
}
