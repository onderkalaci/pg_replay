
#include "postgres.h"
#include "miscadmin.h"

#include "libpq-fe.h"


#include "executor/execdesc.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "utils/elog.h"
#include "utils/palloc.h"
#include "utils/builtins.h"
#include "libpq/pqformat.h"
#include "nodes/makefuncs.h"
#include "optimizer/planner.h"
#include "optimizer/optimizer.h"
#include "utils/builtins.h"
#include "tcop/utility.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define MAX_CONN_STR_SIZE 256

/*
 * A single command may trigger multiple commands (like an INSERT may
 * trigger SELECT due to foreign keys, or a function call may trigger
 * many queries).
 *
 * This variable keeps track of the depth of the operation.
 */
static int OperationLevel = 0;

static bool EnablePGReplay = false;

typedef struct ConnectionHashEntry
{
	uint32 nodeId;

	char connectionStr[MAX_CONN_STR_SIZE];
	PGconn *connection;
} ConnectionHashEntry;


/* internal data representation */
typedef struct NodeConnection
{
	uint64 nodeId;

	char connectionStr[MAX_CONN_STR_SIZE];
} NodeConnection;


/* magic PG_Init */
void _PG_init(void);

/* planner hooks */
static PlannedStmt * pg_replay_planner(Query *parse, int cursorOptions, ParamListInfo
									   boundParams);
static PlannedStmt * CallPlanner(Query *parse, int cursorOptions, ParamListInfo
								 boundParams);

/* process utility hool */
static void pg_replay_ProcessUtility(PlannedStmt *pstmt,
									 const char *queryString,
									 ProcessUtilityContext context,
									 ParamListInfo params,
									 struct QueryEnvironment *queryEnv,
									 DestReceiver *dest,
									 char *completionTag);

/* executor hooks */
static void PgReplayExecutorStart(QueryDesc *queryDesc, int eflags);
static void CallExecutorStart(QueryDesc *queryDesc, int eflags);
static void PgReplayExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64
								count, bool execute_once);
static void CallExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64
							count, bool execute_once);
static void PgReplayExecutorEnd(QueryDesc *queryDesc);
static void CallExecutorEnd(QueryDesc *queryDesc);


static void BuildNodeConnectionHash(void);
static void ExecuteCommandViaSPI(char *command);
static List * GetNodeConnStringList(void);
static void ReplayCommandOnNodes(const char *command);

/* hooks that pg_replay use */
/* TODO: use transaction hooks to check connection states */
static planner_hook_type prev_planner_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;

/* keep cached connections */
static HTAB *NodeConnectionHash = NULL;


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

	DefineCustomBoolVariable(
		"pg_replay.enable",
		gettext_noop("Swith on/off the extenion."),
		NULL,
		&EnablePGReplay,
		false,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);
}


static void
PgReplayExecutorStart(QueryDesc *queryDesc, int eflags)
{
	if (!EnablePGReplay)
	{
		CallExecutorStart(queryDesc, eflags);

		return;
	}

	OperationLevel++;

	PG_TRY();
	{
		CallExecutorStart(queryDesc, eflags);
	}
	PG_CATCH();
	{
		OperationLevel--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}


/*
 * Wrapper around executorStart functions.
 */
static void
CallExecutorStart(QueryDesc *queryDesc, int eflags)
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


static void
PgReplayExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count,
					bool execute_once)
{
	if (!EnablePGReplay)
	{
		CallExecutorRun(queryDesc, direction, count, execute_once);

		return;
	}

	PG_TRY();
	{
		CallExecutorRun(queryDesc, direction, count, execute_once);
	}
	PG_CATCH();
	{
		OperationLevel--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}


/*
 * Wrapper around executorRun functions.
 */
static void
CallExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64
				count, bool execute_once)
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


static void
PgReplayExecutorEnd(QueryDesc *queryDesc)
{
	if (!EnablePGReplay)
	{
		CallExecutorEnd(queryDesc);
		return;
	}

	PG_TRY();
	{
		CallExecutorEnd(queryDesc);
	}
	PG_CATCH();
	{
		OperationLevel--;
		PG_RE_THROW();
	}
	PG_END_TRY();
	OperationLevel--;
}


/*
 * Wrapper around executorEnd functions.
 */
static void
CallExecutorEnd(QueryDesc *queryDesc)
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


/*
 * A simple utility hook, keeping track of OperationLevel.
 */
static void
pg_replay_ProcessUtility(PlannedStmt *pstmt,
						 const char *queryString,
						 ProcessUtilityContext context,
						 ParamListInfo params,
						 struct QueryEnvironment *queryEnv,
						 DestReceiver *dest,
						 char *completionTag)
{
	if (!EnablePGReplay)
	{
		standard_ProcessUtility(pstmt, queryString, context,
								params, queryEnv, dest, completionTag);

		return;
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

	/*
	 * We only send operations with level zero as a single command may
	 * cascade into multiple commands, and we don't want to send all of them.
	 * If we send the top level command, it is expected to cascade into other
	 * commands on the remote end as well.
	 */
	if (OperationLevel == 0)
	{
		ReplayCommandOnNodes(queryString);
	}
}


static PlannedStmt *
pg_replay_planner(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
	if (!EnablePGReplay)
	{
		return CallPlanner(parse, cursorOptions, boundParams);
	}

	PlannedStmt *result = NULL;

	++OperationLevel;

	PG_TRY();
	{
		result = CallPlanner(parse, cursorOptions, boundParams);
	}
	PG_CATCH();
	{
		OperationLevel--;
		PG_RE_THROW();
	}
	PG_END_TRY();

	OperationLevel--;
	if (OperationLevel == 0)
	{
		/* TODO: deparse query and send it */
		ReplayCommandOnNodes("SELECT 1");
	}

	return result;
}


/*
 * A simple wrapper around calling planner.
 */
static PlannedStmt *
CallPlanner(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
	if (prev_planner_hook)
	{
		return prev_planner_hook(parse, cursorOptions,
								 boundParams);
	}
	else
	{
		return standard_planner(parse, cursorOptions,
								boundParams);
	}
}


/*
 * External API to add replay targets.
 *
 * TODO: check whether the connection string is valid or not.
 */
Datum
add_replay_target(PG_FUNCTION_ARGS)
{
	text *replayHostStr = PG_GETARG_TEXT_P(0);
	char *replayHost = text_to_cstring(replayHostStr);

	StringInfo str = makeStringInfo();

	appendStringInfo(str,
					 "INSERT INTO replay_internal.replay_targets (conn_str) VALUES ('%s')",
					 replayHost);

	ExecuteCommandViaSPI(str->data);

	PG_RETURN_VOID();
}


/*
 * External API to add remove targets.
 *
 * TODO: check whether the connection string is valid or not.
 */
Datum
remove_replay_target(PG_FUNCTION_ARGS)
{
	text *replayHostStr = PG_GETARG_TEXT_P(0);
	char *replayHost = text_to_cstring(replayHostStr);

	StringInfo str = makeStringInfo();

	appendStringInfo(str,
					 "DELETE FROM replay_internal.replay_targets WHERE conn_str = '%s'",
					 replayHost);

	ExecuteCommandViaSPI(str->data);

	PG_RETURN_VOID();
}


/*
 * ExecuteCommandViaSPI gets the command and executes it via SPI.
 */
static void
ExecuteCommandViaSPI(char *command)
{
	int connected = SPI_connect();
	if (connected != SPI_OK_CONNECT)
	{
		ereport(ERROR, (errmsg("could not connect to SPI manager")));
	}

	int setSearchPathResult = SPI_exec(command, 0);
	if (setSearchPathResult < 0)
	{
		ereport(ERROR, (errmsg("execution was not successful \"%s\"", command)));
	}

	SPI_finish();
}


/*
 * Read the metadata and return a list of target nodes that we can use.
 */
static List *
GetNodeConnStringList(void)
{
	if (!EnablePGReplay)
	{
		return NIL;
	}

	List *nodeList = NIL;
	int connected = SPI_connect();
	if (connected != SPI_OK_CONNECT)
	{
		ereport(WARNING, (errmsg("could not connect to SPI manager")));

		return NIL;
	}

	char *getConnStrCommand =
		"SELECT node_id, conn_str FROM replay_internal.replay_targets";
	int getConnStrCommandResult = SPI_exec(getConnStrCommand, 0);
	if (getConnStrCommandResult < 0)
	{
		ereport(WARNING, (errmsg("execution was not successful \"%s\"",
								 getConnStrCommand)));

		return NIL;
	}

	int tupleIndex = 0;
	for (tupleIndex = 0; tupleIndex < SPI_processed; tupleIndex++)
	{
		HeapTuple tuple = SPI_tuptable->vals[tupleIndex];

		int nodeIdIndex = SPI_fnumber(SPI_tuptable->tupdesc, "node_id");
		char *nodeIdStr = SPI_getvalue(tuple, SPI_tuptable->tupdesc, nodeIdIndex);

		int connStrIndex = SPI_fnumber(SPI_tuptable->tupdesc, "conn_str");
		char *connStr = SPI_getvalue(tuple, SPI_tuptable->tupdesc, connStrIndex);

		/* TODO: allocate in the connection context */
		MemoryContext oldContext = MemoryContextSwitchTo(TopMemoryContext);

		NodeConnection *nodeConnection = palloc0(sizeof(NodeConnection));

		nodeConnection->nodeId = atoi(nodeIdStr);
		strlcpy(nodeConnection->connectionStr, connStr, MAX_CONN_STR_SIZE);

		nodeList = lappend(nodeList, nodeConnection);

		MemoryContextSwitchTo(oldContext);
	}

	SPI_finish();

	return nodeList;
}


/*
 * Replay the input command on the remote
 */
static void
ReplayCommandOnNodes(const char *command)
{
	if (NodeConnectionHash == NULL)
	{
		BuildNodeConnectionHash();

		Assert(NodeConnectionHash != NULL);
	}

	HASH_SEQ_STATUS status;
	ConnectionHashEntry *entry;

	hash_seq_init(&status, NodeConnectionHash);
	while ((entry = (ConnectionHashEntry *) hash_seq_search(&status)) != 0)
	{
		/* TODO: should we error out as it might break TXes */
		if (PQstatus(entry->connection) != CONNECTION_OK)
		{
			PQfinish(entry->connection);

			entry->connection = NULL;
		}

		/*
		 * TODO: We should not re-connect at this point as this would break transactions.
		 */
		if (entry->connection == NULL)
		{
			entry->connection = PQconnectdb(entry->connectionStr);
		}

		elog(DEBUG1, "Replaying command \"%s\" to node \"%s\"", command,
			 entry->connectionStr);
		PGresult *res = PQexec(entry->connection, command);
		PQclear(res);
	}
}


/*
 * Fill the NodeConnectionHash.
 */
static void
BuildNodeConnectionHash(void)
{
	HASHCTL info;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(uint32);
	info.entrysize = sizeof(ConnectionHashEntry);
	info.hash = uint32_hash;
	uint32 hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	NodeConnectionHash =
		hash_create("pg_replay connection cache", 8, &info, hashFlags);

	MemoryContext currentContext = CurrentMemoryContext;

	/* TODO: find a proper memory context */
	MemoryContextSwitchTo(TopMemoryContext);

	List *nodeList = GetNodeConnStringList();
	ListCell *nodeCell = NULL;
	foreach(nodeCell, nodeList)
	{
		NodeConnection *node = lfirst(nodeCell);
		bool found = false;

		ConnectionHashEntry *entry = hash_search(NodeConnectionHash, &(node->nodeId),
												 HASH_ENTER, &found);

		strlcpy(entry->connectionStr, node->connectionStr, MAX_CONN_STR_SIZE);

		Assert(!found);
	}

	MemoryContextSwitchTo(currentContext);
}
