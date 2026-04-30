/* This is a generated file, edit clickhouse.stub.php instead.
 * Stub hash: 0540ef51645d51aa12734fa9b708ee7a6d326fa0 */

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_ClickHouse___construct, 0, 0, 1)
	ZEND_ARG_TYPE_INFO(0, connectParams, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_ClickHouse___destruct, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_select, 0, 1, IS_MIXED, 0)
	ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, params, IS_ARRAY, 0, "[]")
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, fetch_mode, IS_LONG, 0, "0")
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, query_id, IS_STRING, 0, "\"\"")
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, settings, IS_ARRAY, 0, "[]")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_insert, 0, 3, _IS_BOOL, 0)
	ZEND_ARG_TYPE_INFO(0, table, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, columns, IS_ARRAY, 0)
	ZEND_ARG_TYPE_INFO(0, values, IS_ARRAY, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, query_id, IS_STRING, 0, "\"\"")
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, settings, IS_ARRAY, 0, "[]")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_insertAssoc, 0, 2, _IS_BOOL, 0)
	ZEND_ARG_TYPE_INFO(0, table, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, rows, IS_ARRAY, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, query_id, IS_STRING, 0, "\"\"")
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, settings, IS_ARRAY, 0, "[]")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_writeStart, 0, 2, _IS_BOOL, 0)
	ZEND_ARG_TYPE_INFO(0, table, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, columns, IS_ARRAY, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, query_id, IS_STRING, 0, "\"\"")
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, settings, IS_ARRAY, 0, "[]")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_write, 0, 1, _IS_BOOL, 0)
	ZEND_ARG_TYPE_INFO(0, values, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_writeEnd, 0, 0, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_execute, 0, 1, _IS_BOOL, 0)
	ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, params, IS_ARRAY, 0, "[]")
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, query_id, IS_STRING, 0, "\"\"")
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, settings, IS_ARRAY, 0, "[]")
ZEND_END_ARG_INFO()

#define arginfo_class_ClickHouse_ping arginfo_class_ClickHouse_writeEnd

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_setSettings, 0, 1, IS_STATIC, 0)
	ZEND_ARG_TYPE_INFO(0, settings, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_setSetting, 0, 2, IS_STATIC, 0)
	ZEND_ARG_TYPE_INFO(0, key, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, value, IS_MIXED, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_setDatabase, 0, 1, IS_STATIC, 0)
	ZEND_ARG_TYPE_INFO(0, database, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_setProgressCallback, 0, 1, _IS_BOOL, 0)
	ZEND_ARG_TYPE_INFO(0, callback, IS_CALLABLE, 1)
ZEND_END_ARG_INFO()

#define arginfo_class_ClickHouse_setProfileCallback arginfo_class_ClickHouse_setProgressCallback

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_setVerbose, 0, 1, IS_STATIC, 0)
	ZEND_ARG_TYPE_INFO(0, sink, IS_MIXED, 0)
ZEND_END_ARG_INFO()

#define arginfo_class_ClickHouse_resetConnection arginfo_class_ClickHouse_writeEnd

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_getServerInfo, 0, 0, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_getCurrentEndpoint, 0, 0, IS_ARRAY, 1)
ZEND_END_ARG_INFO()

#define arginfo_class_ClickHouse_getStatistics arginfo_class_ClickHouse_getServerInfo

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_databaseSize, 0, 0, IS_ARRAY, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, database, IS_STRING, 1, "null")
ZEND_END_ARG_INFO()

#define arginfo_class_ClickHouse_tablesSize arginfo_class_ClickHouse_databaseSize

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_partitions, 0, 1, IS_ARRAY, 0)
	ZEND_ARG_TYPE_INFO(0, table, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_showTables, 0, 0, IS_ARRAY, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, database, IS_STRING, 1, "null")
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, like, IS_STRING, 1, "null")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_showCreateTable, 0, 1, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, table, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_getServerUptime, 0, 0, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_enableLogQueries, 0, 0, _IS_BOOL, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, enabled, _IS_BOOL, 0, "true")
ZEND_END_ARG_INFO()

#define arginfo_class_ClickHouse_getLogQueries arginfo_class_ClickHouse_getServerInfo

ZEND_BEGIN_ARG_WITH_RETURN_OBJ_INFO_EX(arginfo_class_ClickHouse_selectStream, 0, 1, ClickHouseRowIterator, 0)
	ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, params, IS_ARRAY, 0, "[]")
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, query_id, IS_STRING, 0, "\"\"")
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, settings, IS_ARRAY, 0, "[]")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_OBJ_INFO_EX(arginfo_class_ClickHouse_selectStatement, 0, 1, ClickHouseStatement, 0)
	ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, params, IS_ARRAY, 0, "[]")
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, query_id, IS_STRING, 0, "\"\"")
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, settings, IS_ARRAY, 0, "[]")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_selectStreamCallback, 0, 2, _IS_BOOL, 0)
	ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, callback, IS_CALLABLE, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, params, IS_ARRAY, 0, "[]")
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, query_id, IS_STRING, 0, "\"\"")
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, settings, IS_ARRAY, 0, "[]")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_isExists, 0, 2, _IS_BOOL, 0)
	ZEND_ARG_TYPE_INFO(0, database, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, table, IS_STRING, 0)
ZEND_END_ARG_INFO()

#define arginfo_class_ClickHouse_showDatabases arginfo_class_ClickHouse_getServerInfo

#define arginfo_class_ClickHouse_showProcesslist arginfo_class_ClickHouse_getServerInfo

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_getServerVersion, 0, 0, IS_STRING, 0)
ZEND_END_ARG_INFO()

#define arginfo_class_ClickHouse_tableSize arginfo_class_ClickHouse_partitions

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_truncateTable, 0, 1, _IS_BOOL, 0)
	ZEND_ARG_TYPE_INFO(0, table, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_dropPartition, 0, 2, _IS_BOOL, 0)
	ZEND_ARG_TYPE_INFO(0, table, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, partition, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouseRowIterator_rewind, 0, 0, IS_VOID, 0)
ZEND_END_ARG_INFO()

#define arginfo_class_ClickHouseRowIterator_valid arginfo_class_ClickHouse_writeEnd

#define arginfo_class_ClickHouseRowIterator_current arginfo_class_ClickHouse_getServerInfo

#define arginfo_class_ClickHouseRowIterator_key arginfo_class_ClickHouse_getServerUptime

#define arginfo_class_ClickHouseRowIterator_next arginfo_class_ClickHouseRowIterator_rewind

#define arginfo_class_ClickHouseRowIterator_count arginfo_class_ClickHouse_getServerUptime

#define arginfo_class_ClickHouseStatement___construct arginfo_class_ClickHouse___destruct

#define arginfo_class_ClickHouseStatement_count arginfo_class_ClickHouse_getServerUptime

#define arginfo_class_ClickHouseStatement_rewind arginfo_class_ClickHouseRowIterator_rewind

#define arginfo_class_ClickHouseStatement_valid arginfo_class_ClickHouse_writeEnd

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouseStatement_current, 0, 0, IS_MIXED, 0)
ZEND_END_ARG_INFO()

#define arginfo_class_ClickHouseStatement_key arginfo_class_ClickHouseStatement_current

#define arginfo_class_ClickHouseStatement_next arginfo_class_ClickHouseRowIterator_rewind

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouseStatement_offsetExists, 0, 1, _IS_BOOL, 0)
	ZEND_ARG_TYPE_INFO(0, offset, IS_MIXED, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouseStatement_offsetGet, 0, 1, IS_MIXED, 0)
	ZEND_ARG_TYPE_INFO(0, offset, IS_MIXED, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouseStatement_offsetSet, 0, 2, IS_VOID, 0)
	ZEND_ARG_TYPE_INFO(0, offset, IS_MIXED, 0)
	ZEND_ARG_TYPE_INFO(0, value, IS_MIXED, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouseStatement_offsetUnset, 0, 1, IS_VOID, 0)
	ZEND_ARG_TYPE_INFO(0, offset, IS_MIXED, 0)
ZEND_END_ARG_INFO()

#define arginfo_class_ClickHouseStatement_jsonSerialize arginfo_class_ClickHouse_getServerInfo

#define arginfo_class_ClickHouseStatement_toArray arginfo_class_ClickHouse_getServerInfo

#define arginfo_class_ClickHouseStatement_statistics arginfo_class_ClickHouse_getServerInfo

#define arginfo_class_ClickHouseStatement_fetchOne arginfo_class_ClickHouseStatement_current

#define arginfo_class_ClickHouseStatement_fetchKeyPair arginfo_class_ClickHouse_getServerInfo

#define arginfo_class_ClickHouseStatement_fetchColumn arginfo_class_ClickHouse_getServerInfo

#define arginfo_class_ClickHouseException_getServerCode arginfo_class_ClickHouse_getServerUptime

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouseException_getServerName, 0, 0, IS_STRING, 1)
ZEND_END_ARG_INFO()

#define arginfo_class_ClickHouseException_getQueryId arginfo_class_ClickHouseException_getServerName

ZEND_METHOD(ClickHouse, __construct);
ZEND_METHOD(ClickHouse, __destruct);
ZEND_METHOD(ClickHouse, select);
ZEND_METHOD(ClickHouse, insert);
ZEND_METHOD(ClickHouse, insertAssoc);
ZEND_METHOD(ClickHouse, writeStart);
ZEND_METHOD(ClickHouse, write);
ZEND_METHOD(ClickHouse, writeEnd);
ZEND_METHOD(ClickHouse, execute);
ZEND_METHOD(ClickHouse, ping);
ZEND_METHOD(ClickHouse, setSettings);
ZEND_METHOD(ClickHouse, setSetting);
ZEND_METHOD(ClickHouse, setDatabase);
ZEND_METHOD(ClickHouse, setProgressCallback);
ZEND_METHOD(ClickHouse, setProfileCallback);
ZEND_METHOD(ClickHouse, setVerbose);
ZEND_METHOD(ClickHouse, resetConnection);
ZEND_METHOD(ClickHouse, getServerInfo);
ZEND_METHOD(ClickHouse, getCurrentEndpoint);
ZEND_METHOD(ClickHouse, getStatistics);
ZEND_METHOD(ClickHouse, databaseSize);
ZEND_METHOD(ClickHouse, tablesSize);
ZEND_METHOD(ClickHouse, partitions);
ZEND_METHOD(ClickHouse, showTables);
ZEND_METHOD(ClickHouse, showCreateTable);
ZEND_METHOD(ClickHouse, getServerUptime);
ZEND_METHOD(ClickHouse, enableLogQueries);
ZEND_METHOD(ClickHouse, getLogQueries);
ZEND_METHOD(ClickHouse, selectStream);
ZEND_METHOD(ClickHouse, selectStatement);
ZEND_METHOD(ClickHouse, selectStreamCallback);
ZEND_METHOD(ClickHouse, isExists);
ZEND_METHOD(ClickHouse, showDatabases);
ZEND_METHOD(ClickHouse, showProcesslist);
ZEND_METHOD(ClickHouse, getServerVersion);
ZEND_METHOD(ClickHouse, tableSize);
ZEND_METHOD(ClickHouse, truncateTable);
ZEND_METHOD(ClickHouse, dropPartition);
ZEND_METHOD(ClickHouseRowIterator, rewind);
ZEND_METHOD(ClickHouseRowIterator, valid);
ZEND_METHOD(ClickHouseRowIterator, current);
ZEND_METHOD(ClickHouseRowIterator, key);
ZEND_METHOD(ClickHouseRowIterator, next);
ZEND_METHOD(ClickHouseRowIterator, count);
ZEND_METHOD(ClickHouseStatement, __construct);
ZEND_METHOD(ClickHouseStatement, count);
ZEND_METHOD(ClickHouseStatement, rewind);
ZEND_METHOD(ClickHouseStatement, valid);
ZEND_METHOD(ClickHouseStatement, current);
ZEND_METHOD(ClickHouseStatement, key);
ZEND_METHOD(ClickHouseStatement, next);
ZEND_METHOD(ClickHouseStatement, offsetExists);
ZEND_METHOD(ClickHouseStatement, offsetGet);
ZEND_METHOD(ClickHouseStatement, offsetSet);
ZEND_METHOD(ClickHouseStatement, offsetUnset);
ZEND_METHOD(ClickHouseStatement, jsonSerialize);
ZEND_METHOD(ClickHouseStatement, toArray);
ZEND_METHOD(ClickHouseStatement, statistics);
ZEND_METHOD(ClickHouseStatement, fetchOne);
ZEND_METHOD(ClickHouseStatement, fetchKeyPair);
ZEND_METHOD(ClickHouseStatement, fetchColumn);
ZEND_METHOD(ClickHouseException, getServerCode);
ZEND_METHOD(ClickHouseException, getServerName);
ZEND_METHOD(ClickHouseException, getQueryId);

static const zend_function_entry class_ClickHouse_methods[] = {
	ZEND_ME(ClickHouse, __construct, arginfo_class_ClickHouse___construct, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, __destruct, arginfo_class_ClickHouse___destruct, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, select, arginfo_class_ClickHouse_select, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, insert, arginfo_class_ClickHouse_insert, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, insertAssoc, arginfo_class_ClickHouse_insertAssoc, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, writeStart, arginfo_class_ClickHouse_writeStart, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, write, arginfo_class_ClickHouse_write, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, writeEnd, arginfo_class_ClickHouse_writeEnd, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, execute, arginfo_class_ClickHouse_execute, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, ping, arginfo_class_ClickHouse_ping, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, setSettings, arginfo_class_ClickHouse_setSettings, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, setSetting, arginfo_class_ClickHouse_setSetting, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, setDatabase, arginfo_class_ClickHouse_setDatabase, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, setProgressCallback, arginfo_class_ClickHouse_setProgressCallback, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, setProfileCallback, arginfo_class_ClickHouse_setProfileCallback, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, setVerbose, arginfo_class_ClickHouse_setVerbose, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, resetConnection, arginfo_class_ClickHouse_resetConnection, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, getServerInfo, arginfo_class_ClickHouse_getServerInfo, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, getCurrentEndpoint, arginfo_class_ClickHouse_getCurrentEndpoint, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, getStatistics, arginfo_class_ClickHouse_getStatistics, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, databaseSize, arginfo_class_ClickHouse_databaseSize, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, tablesSize, arginfo_class_ClickHouse_tablesSize, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, partitions, arginfo_class_ClickHouse_partitions, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, showTables, arginfo_class_ClickHouse_showTables, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, showCreateTable, arginfo_class_ClickHouse_showCreateTable, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, getServerUptime, arginfo_class_ClickHouse_getServerUptime, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, enableLogQueries, arginfo_class_ClickHouse_enableLogQueries, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, getLogQueries, arginfo_class_ClickHouse_getLogQueries, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, selectStream, arginfo_class_ClickHouse_selectStream, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, selectStatement, arginfo_class_ClickHouse_selectStatement, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, selectStreamCallback, arginfo_class_ClickHouse_selectStreamCallback, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, isExists, arginfo_class_ClickHouse_isExists, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, showDatabases, arginfo_class_ClickHouse_showDatabases, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, showProcesslist, arginfo_class_ClickHouse_showProcesslist, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, getServerVersion, arginfo_class_ClickHouse_getServerVersion, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, tableSize, arginfo_class_ClickHouse_tableSize, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, truncateTable, arginfo_class_ClickHouse_truncateTable, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouse, dropPartition, arginfo_class_ClickHouse_dropPartition, ZEND_ACC_PUBLIC)
	ZEND_FE_END
};

static const zend_function_entry class_ClickHouseRowIterator_methods[] = {
	ZEND_ME(ClickHouseRowIterator, rewind, arginfo_class_ClickHouseRowIterator_rewind, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouseRowIterator, valid, arginfo_class_ClickHouseRowIterator_valid, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouseRowIterator, current, arginfo_class_ClickHouseRowIterator_current, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouseRowIterator, key, arginfo_class_ClickHouseRowIterator_key, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouseRowIterator, next, arginfo_class_ClickHouseRowIterator_next, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouseRowIterator, count, arginfo_class_ClickHouseRowIterator_count, ZEND_ACC_PUBLIC)
	ZEND_FE_END
};

static const zend_function_entry class_ClickHouseStatement_methods[] = {
	ZEND_ME(ClickHouseStatement, __construct, arginfo_class_ClickHouseStatement___construct, ZEND_ACC_PRIVATE)
	ZEND_ME(ClickHouseStatement, count, arginfo_class_ClickHouseStatement_count, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouseStatement, rewind, arginfo_class_ClickHouseStatement_rewind, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouseStatement, valid, arginfo_class_ClickHouseStatement_valid, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouseStatement, current, arginfo_class_ClickHouseStatement_current, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouseStatement, key, arginfo_class_ClickHouseStatement_key, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouseStatement, next, arginfo_class_ClickHouseStatement_next, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouseStatement, offsetExists, arginfo_class_ClickHouseStatement_offsetExists, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouseStatement, offsetGet, arginfo_class_ClickHouseStatement_offsetGet, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouseStatement, offsetSet, arginfo_class_ClickHouseStatement_offsetSet, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouseStatement, offsetUnset, arginfo_class_ClickHouseStatement_offsetUnset, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouseStatement, jsonSerialize, arginfo_class_ClickHouseStatement_jsonSerialize, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouseStatement, toArray, arginfo_class_ClickHouseStatement_toArray, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouseStatement, statistics, arginfo_class_ClickHouseStatement_statistics, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouseStatement, fetchOne, arginfo_class_ClickHouseStatement_fetchOne, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouseStatement, fetchKeyPair, arginfo_class_ClickHouseStatement_fetchKeyPair, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouseStatement, fetchColumn, arginfo_class_ClickHouseStatement_fetchColumn, ZEND_ACC_PUBLIC)
	ZEND_FE_END
};

static const zend_function_entry class_ClickHouseException_methods[] = {
	ZEND_ME(ClickHouseException, getServerCode, arginfo_class_ClickHouseException_getServerCode, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouseException, getServerName, arginfo_class_ClickHouseException_getServerName, ZEND_ACC_PUBLIC)
	ZEND_ME(ClickHouseException, getQueryId, arginfo_class_ClickHouseException_getQueryId, ZEND_ACC_PUBLIC)
	ZEND_FE_END
};

static zend_class_entry *register_class_ClickHouse(void)
{
	zend_class_entry ce, *class_entry;

	INIT_CLASS_ENTRY(ce, "ClickHouse", class_ClickHouse_methods);
	class_entry = zend_register_internal_class_with_flags(&ce, NULL, ZEND_ACC_FINAL);

	zval const_FETCH_ONE_value;
	ZVAL_LONG(&const_FETCH_ONE_value, 1);
	zend_string *const_FETCH_ONE_name = zend_string_init_interned("FETCH_ONE", sizeof("FETCH_ONE") - 1, true);
	zend_declare_typed_class_constant(class_entry, const_FETCH_ONE_name, &const_FETCH_ONE_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_LONG));
	zend_string_release_ex(const_FETCH_ONE_name, true);

	zval const_FETCH_KEY_PAIR_value;
	ZVAL_LONG(&const_FETCH_KEY_PAIR_value, 2);
	zend_string *const_FETCH_KEY_PAIR_name = zend_string_init_interned("FETCH_KEY_PAIR", sizeof("FETCH_KEY_PAIR") - 1, true);
	zend_declare_typed_class_constant(class_entry, const_FETCH_KEY_PAIR_name, &const_FETCH_KEY_PAIR_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_LONG));
	zend_string_release_ex(const_FETCH_KEY_PAIR_name, true);

	zval const_DATE_AS_STRINGS_value;
	ZVAL_LONG(&const_DATE_AS_STRINGS_value, 4);
	zend_string *const_DATE_AS_STRINGS_name = zend_string_init_interned("DATE_AS_STRINGS", sizeof("DATE_AS_STRINGS") - 1, true);
	zend_declare_typed_class_constant(class_entry, const_DATE_AS_STRINGS_name, &const_DATE_AS_STRINGS_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_LONG));
	zend_string_release_ex(const_DATE_AS_STRINGS_name, true);

	zval const_FETCH_COLUMN_value;
	ZVAL_LONG(&const_FETCH_COLUMN_value, 8);
	zend_string *const_FETCH_COLUMN_name = zend_string_init_interned("FETCH_COLUMN", sizeof("FETCH_COLUMN") - 1, true);
	zend_declare_typed_class_constant(class_entry, const_FETCH_COLUMN_name, &const_FETCH_COLUMN_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_LONG));
	zend_string_release_ex(const_FETCH_COLUMN_name, true);

	zval property_host_default_value;
	zend_string *property_host_default_value_str = zend_string_init("127.0.0.1", strlen("127.0.0.1"), 1);
	ZVAL_STR(&property_host_default_value, property_host_default_value_str);
	zend_declare_typed_property(class_entry, ZSTR_KNOWN(ZEND_STR_HOST), &property_host_default_value, ZEND_ACC_PROTECTED, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_STRING));

	zval property_port_default_value;
	ZVAL_LONG(&property_port_default_value, 9000);
	zend_declare_typed_property(class_entry, ZSTR_KNOWN(ZEND_STR_PORT), &property_port_default_value, ZEND_ACC_PROTECTED, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_LONG));

	zval property_database_default_value;
	zend_string *property_database_default_value_str = zend_string_init("default", strlen("default"), 1);
	ZVAL_STR(&property_database_default_value, property_database_default_value_str);
	zend_string *property_database_name = zend_string_init("database", sizeof("database") - 1, true);
	zend_declare_typed_property(class_entry, property_database_name, &property_database_default_value, ZEND_ACC_PROTECTED, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_STRING));
	zend_string_release_ex(property_database_name, true);

	zval property_user_default_value;
	ZVAL_NULL(&property_user_default_value);
	zend_declare_typed_property(class_entry, ZSTR_KNOWN(ZEND_STR_USER), &property_user_default_value, ZEND_ACC_PROTECTED, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_STRING|MAY_BE_NULL));

	zval property_compression_default_value;
	ZVAL_LONG(&property_compression_default_value, 0);
	zend_string *property_compression_name = zend_string_init("compression", sizeof("compression") - 1, true);
	zend_declare_typed_property(class_entry, property_compression_name, &property_compression_default_value, ZEND_ACC_PROTECTED, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_LONG));
	zend_string_release_ex(property_compression_name, true);

	zval property_retry_timeout_default_value;
	ZVAL_LONG(&property_retry_timeout_default_value, 5);
	zend_string *property_retry_timeout_name = zend_string_init("retry_timeout", sizeof("retry_timeout") - 1, true);
	zend_declare_typed_property(class_entry, property_retry_timeout_name, &property_retry_timeout_default_value, ZEND_ACC_PROTECTED, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_LONG));
	zend_string_release_ex(property_retry_timeout_name, true);

	zval property_retry_count_default_value;
	ZVAL_LONG(&property_retry_count_default_value, 1);
	zend_string *property_retry_count_name = zend_string_init("retry_count", sizeof("retry_count") - 1, true);
	zend_declare_typed_property(class_entry, property_retry_count_name, &property_retry_count_default_value, ZEND_ACC_PROTECTED, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_LONG));
	zend_string_release_ex(property_retry_count_name, true);

	zval property_receive_timeout_default_value;
	ZVAL_LONG(&property_receive_timeout_default_value, 0);
	zend_string *property_receive_timeout_name = zend_string_init("receive_timeout", sizeof("receive_timeout") - 1, true);
	zend_declare_typed_property(class_entry, property_receive_timeout_name, &property_receive_timeout_default_value, ZEND_ACC_PROTECTED, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_LONG));
	zend_string_release_ex(property_receive_timeout_name, true);

	zval property_connect_timeout_default_value;
	ZVAL_LONG(&property_connect_timeout_default_value, 5);
	zend_string *property_connect_timeout_name = zend_string_init("connect_timeout", sizeof("connect_timeout") - 1, true);
	zend_declare_typed_property(class_entry, property_connect_timeout_name, &property_connect_timeout_default_value, ZEND_ACC_PROTECTED, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_LONG));
	zend_string_release_ex(property_connect_timeout_name, true);

	return class_entry;
}

static zend_class_entry *register_class_ClickHouseRowIterator(zend_class_entry *class_entry_Iterator, zend_class_entry *class_entry_Countable)
{
	zend_class_entry ce, *class_entry;

	INIT_CLASS_ENTRY(ce, "ClickHouseRowIterator", class_ClickHouseRowIterator_methods);
	class_entry = zend_register_internal_class_with_flags(&ce, NULL, ZEND_ACC_FINAL);
	zend_class_implements(class_entry, 2, class_entry_Iterator, class_entry_Countable);

	return class_entry;
}

static zend_class_entry *register_class_ClickHouseStatement(zend_class_entry *class_entry_Iterator, zend_class_entry *class_entry_Countable, zend_class_entry *class_entry_ArrayAccess, zend_class_entry *class_entry_JsonSerializable)
{
	zend_class_entry ce, *class_entry;

	INIT_CLASS_ENTRY(ce, "ClickHouseStatement", class_ClickHouseStatement_methods);
	class_entry = zend_register_internal_class_with_flags(&ce, NULL, ZEND_ACC_FINAL);
	zend_class_implements(class_entry, 4, class_entry_Iterator, class_entry_Countable, class_entry_ArrayAccess, class_entry_JsonSerializable);

	return class_entry;
}

static zend_class_entry *register_class_ClickHouseException(zend_class_entry *class_entry_Exception)
{
	zend_class_entry ce, *class_entry;

	INIT_CLASS_ENTRY(ce, "ClickHouseException", class_ClickHouseException_methods);
	class_entry = zend_register_internal_class_with_flags(&ce, class_entry_Exception, 0);

	zval property_server_code_default_value;
	ZVAL_LONG(&property_server_code_default_value, 0);
	zend_string *property_server_code_name = zend_string_init("server_code", sizeof("server_code") - 1, true);
	zend_declare_typed_property(class_entry, property_server_code_name, &property_server_code_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_LONG));
	zend_string_release_ex(property_server_code_name, true);

	zval property_server_name_default_value;
	ZVAL_NULL(&property_server_name_default_value);
	zend_string *property_server_name_name = zend_string_init("server_name", sizeof("server_name") - 1, true);
	zend_declare_typed_property(class_entry, property_server_name_name, &property_server_name_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_STRING|MAY_BE_NULL));
	zend_string_release_ex(property_server_name_name, true);

	zval property_query_id_default_value;
	ZVAL_NULL(&property_query_id_default_value);
	zend_string *property_query_id_name = zend_string_init("query_id", sizeof("query_id") - 1, true);
	zend_declare_typed_property(class_entry, property_query_id_name, &property_query_id_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_STRING|MAY_BE_NULL));
	zend_string_release_ex(property_query_id_name, true);

	return class_entry;
}
