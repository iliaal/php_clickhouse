/*
  +----------------------------------------------------------------------+
  | php_clickhouse                                                       |
  +----------------------------------------------------------------------+
  | Copyright (c) 1997-2026 The PHP Group                                |
  +----------------------------------------------------------------------+
  | This source file is subject to version 3.01 of the PHP license,      |
  | that is bundled with this package in the file LICENSE, and is        |
  | available through the world-wide-web at the following url:           |
  | http://www.php.net/license/3_01.txt                                  |
  | If you did not receive a copy of the PHP license and are unable to   |
  | obtain it through the world-wide-web, please send a note to          |
  | license@php.net so we can mail you a copy immediately.               |
  +----------------------------------------------------------------------+
  | Author: Ilia Alshanetsky <ilia@ilia.ws>                              |
  +----------------------------------------------------------------------+
*/

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "clickhouse_c_internal.h"
#include "php_ini.h"
#include "ext/standard/info.h"
#include "Zend/zend_interfaces.h"
#include <limits.h>
#include <math.h>
#include <string.h>

#include "clickhouse_arginfo.h"

zend_class_entry *clickhouse_ce;
zend_class_entry *clickhouse_exception_ce;
zend_class_entry *clickhouse_iter_ce;
zend_class_entry *clickhouse_statement_ce;

static zend_object_handlers clickhouse_object_handlers;

static zend_object *clickhouse_create_object(zend_class_entry *ce)
{
	clickhouse_object *obj = zend_object_alloc(sizeof(*obj), ce);
	memset(obj, 0, XtOffsetOf(clickhouse_object, std));
	zend_hash_init(&obj->settings, 0, NULL, ZVAL_PTR_DTOR, 0);
	zend_hash_init(&obj->query_log, 0, NULL, ZVAL_PTR_DTOR, 0);
	ZVAL_UNDEF(&obj->progress_callback);
	ZVAL_UNDEF(&obj->profile_callback);
	ZVAL_UNDEF(&obj->verbose_callback);
	zend_object_std_init(&obj->std, ce);
	object_properties_init(&obj->std, ce);
	obj->std.handlers = &clickhouse_object_handlers;
	return &obj->std;
}

static void ch_clear_callback(zval *callback)
{
	zval old;
	ZVAL_COPY_VALUE(&old, callback);
	ZVAL_UNDEF(callback);
	if (!Z_ISUNDEF(old)) {
		zval_ptr_dtor(&old);
	}
}

static void clickhouse_free_obj(zend_object *object)
{
	clickhouse_object *obj = clickhouse_from_obj(object);
	ch_insert_abandon(obj);
	ch_insert_clear(obj);
	ch_backend_destroy(obj->pending_backend);
	obj->pending_backend = NULL;
	ch_backend_destroy(obj->backend);
	obj->backend = NULL;
	ch_clear_callback(&obj->progress_callback);
	ch_clear_callback(&obj->profile_callback);
	ch_clear_callback(&obj->verbose_callback);
	ch_stats_reset(&obj->stats);
	ch_failure_clear(obj);
	zend_hash_destroy(&obj->settings);
	zend_hash_destroy(&obj->query_log);
	zend_object_std_dtor(&obj->std);
}

#if PHP_VERSION_ID >= 80000
static HashTable *clickhouse_get_gc(zend_object *object, zval **table, int *n)
{
	clickhouse_object *obj = clickhouse_from_obj(object);
	zend_get_gc_buffer *buffer = zend_get_gc_buffer_create();
	if (!Z_ISUNDEF(obj->progress_callback)) {
		zend_get_gc_buffer_add_zval(buffer, &obj->progress_callback);
	}
	if (!Z_ISUNDEF(obj->profile_callback)) {
		zend_get_gc_buffer_add_zval(buffer, &obj->profile_callback);
	}
	if (!Z_ISUNDEF(obj->verbose_callback)) {
		zend_get_gc_buffer_add_zval(buffer, &obj->verbose_callback);
	}
	zend_get_gc_buffer_use(buffer, table, n);
	return zend_std_get_properties(object);
}
#else
static HashTable *clickhouse_get_gc(zval *object, zval **table, int *n)
{
	clickhouse_object *obj = Z_CLICKHOUSE_P(object);
	int count = 0;
	if (!Z_ISUNDEF(obj->progress_callback)) {
		ZVAL_COPY_VALUE(&obj->gc_buf[count++], &obj->progress_callback);
	}
	if (!Z_ISUNDEF(obj->profile_callback)) {
		ZVAL_COPY_VALUE(&obj->gc_buf[count++], &obj->profile_callback);
	}
	if (!Z_ISUNDEF(obj->verbose_callback)) {
		ZVAL_COPY_VALUE(&obj->gc_buf[count++], &obj->verbose_callback);
	}
	*table = obj->gc_buf;
	*n = count;
	return zend_std_get_properties(object);
}
#endif

#if PHP_VERSION_ID < 80100
static int clickhouse_serialize_deny(zval *object, unsigned char **buffer,
	size_t *length, zend_serialize_data *data)
{
	(void)buffer;
	(void)length;
	(void)data;
	zend_throw_exception_ex(NULL, 0, "Serialization of '%s' is not allowed",
		ZSTR_VAL(Z_OBJCE_P(object)->name));
	return FAILURE;
}

static int clickhouse_unserialize_deny(zval *object, zend_class_entry *ce,
	const unsigned char *buffer, size_t length, zend_unserialize_data *data)
{
	(void)object;
	(void)buffer;
	(void)length;
	(void)data;
	zend_throw_exception_ex(NULL, 0, "Unserialization of '%s' is not allowed", ZSTR_VAL(ce->name));
	return FAILURE;
}
#endif

static void clickhouse_mark_not_serializable(zend_class_entry *ce)
{
#if PHP_VERSION_ID >= 80100
	ce->ce_flags |= ZEND_ACC_NOT_SERIALIZABLE;
#else
	ce->serialize = clickhouse_serialize_deny;
	ce->unserialize = clickhouse_unserialize_deny;
#endif
}

PHP_RINIT_FUNCTION(clickhouse)
{
#if defined(COMPILE_DL_CLICKHOUSE) && defined(ZTS)
	ZEND_TSRMLS_CACHE_UPDATE();
#endif
	return SUCCESS;
}

PHP_MINIT_FUNCTION(clickhouse)
{
	zend_class_entry *json_serializable_ce;
#if defined(COMPILE_DL_CLICKHOUSE) && defined(ZTS)
	ZEND_TSRMLS_CACHE_UPDATE();
#endif
	clickhouse_ce = register_class_ClickHouse();
	clickhouse_mark_not_serializable(clickhouse_ce);
	clickhouse_ce->create_object = clickhouse_create_object;
	memcpy(&clickhouse_object_handlers, zend_get_std_object_handlers(), sizeof(clickhouse_object_handlers));
	clickhouse_object_handlers.offset = XtOffsetOf(clickhouse_object, std);
	clickhouse_object_handlers.free_obj = clickhouse_free_obj;
	clickhouse_object_handlers.get_gc = clickhouse_get_gc;
	clickhouse_object_handlers.clone_obj = NULL;
#if PHP_VERSION_ID >= 80400
	clickhouse_ce->default_object_handlers = &clickhouse_object_handlers;
#endif
	clickhouse_exception_ce = register_class_ClickHouseException(zend_ce_exception);
	clickhouse_iter_ce = register_class_ClickHouseRowIterator(zend_ce_iterator, zend_ce_countable);
	clickhouse_mark_not_serializable(clickhouse_iter_ce);
	json_serializable_ce = zend_hash_str_find_ptr(CG(class_table), "jsonserializable", sizeof("jsonserializable") - 1);
	if (!json_serializable_ce) {
		return FAILURE;
	}
	clickhouse_statement_ce = register_class_ClickHouseStatement(zend_ce_iterator,
		zend_ce_countable, zend_ce_arrayaccess, json_serializable_ce);
	clickhouse_mark_not_serializable(clickhouse_statement_ce);
	if (ch_results_minit(INIT_FUNC_ARGS_PASSTHRU) == FAILURE) {
		return FAILURE;
	}
	zend_register_class_alias(CLICKHOUSE_RES_NAME_LEGACY, clickhouse_ce);
	zend_register_class_alias(CLICKHOUSE_EXCEPTION_NAME_LEGACY, clickhouse_exception_ce);
	return SUCCESS;
}

PHP_MINFO_FUNCTION(clickhouse)
{
	php_info_print_table_start();
	php_info_print_table_header(2, "ClickHouse support", "enabled");
	php_info_print_table_row(2, "Version", PHP_CLICKHOUSE_VERSION);
	php_info_print_table_row(2, "Author", "SeasX Group[email: ahhhh.wang@gmail.com], Ilia Alshanetsky");
	php_info_print_table_end();
	DISPLAY_INI_ENTRIES();
}

static const zend_function_entry clickhouse_functions[] = { PHP_FE_END };
static const zend_module_dep clickhouse_deps[] = {
	ZEND_MOD_REQUIRED("json")
	ZEND_MOD_END
};

zend_module_entry clickhouse_module_entry = {
	STANDARD_MODULE_HEADER_EX, NULL, clickhouse_deps,
	CLICKHOUSE_RES_NAME, clickhouse_functions,
	PHP_MINIT(clickhouse), NULL, PHP_RINIT(clickhouse), NULL,
	PHP_MINFO(clickhouse), PHP_CLICKHOUSE_VERSION, STANDARD_MODULE_PROPERTIES
};

#ifdef COMPILE_DL_CLICKHOUSE
#ifdef ZTS
ZEND_TSRMLS_CACHE_DEFINE()
#endif
ZEND_GET_MODULE(clickhouse)
#endif

static int ch_config_error(const char *message)
{
	if (!EG(exception)) {
		zend_throw_exception(clickhouse_exception_ce, message, 0);
	}
	return FAILURE;
}

static zval *ch_config_value(HashTable *config, const char *key)
{
	zval *value = zend_hash_str_find(config, key, strlen(key));
	if (value) {
		ZVAL_DEREF(value);
		if (Z_TYPE_P(value) == IS_NULL) {
			return NULL;
		}
	}
	return value;
}

static void ch_snapshot_array(HashTable *out, HashTable *source)
{
	zend_string *key;
	zend_ulong index;
	zval *value;
	zend_hash_init(out, zend_hash_num_elements(source), NULL, ZVAL_PTR_DTOR, 0);
	ZEND_HASH_FOREACH_KEY_VAL(source, index, key, value) {
		zval copy;
		ZVAL_COPY_DEREF(&copy, value);
		if (key) {
			zend_hash_update(out, key, &copy);
		} else {
			zend_hash_index_update(out, index, &copy);
		}
	} ZEND_HASH_FOREACH_END();
}

static bool ch_parse_config_bool(zval *value, bool *out)
{
	ZVAL_DEREF(value);
	switch (Z_TYPE_P(value)) {
	case IS_TRUE:
		*out = true;
		return true;
	case IS_FALSE:
		*out = false;
		return true;
	case IS_LONG:
		if (Z_LVAL_P(value) != 0 && Z_LVAL_P(value) != 1) return false;
		*out = Z_LVAL_P(value) == 1;
		return true;
	case IS_STRING: {
		const char *s = Z_STRVAL_P(value);
		size_t n = Z_STRLEN_P(value);
		if ((n == 1 && s[0] == '1') || (n == 4 && strncasecmp(s, "true", 4) == 0)) {
			*out = true;
			return true;
		}
		if (n == 0 || (n == 1 && s[0] == '0') || (n == 5 && strncasecmp(s, "false", 5) == 0)) {
			*out = false;
			return true;
		}
		return false;
	}
	default:
		return false;
	}
}

static int ch_load_config_bool(HashTable *config, const char *key, bool *out)
{
	zval *value = ch_config_value(config, key);
	if (!value) return SUCCESS;
	if (!ch_parse_config_bool(value, out)) {
		zend_throw_exception_ex(clickhouse_exception_ce, 0,
			"%s must be a boolean (true, false, 0, 1, \"true\", \"false\", \"0\", \"1\", or \"\" for false)", key);
		return FAILURE;
	}
	return SUCCESS;
}

static bool ch_unset_tls_path(const char *key, zval *value)
{
	ZVAL_DEREF(value);
	switch (Z_TYPE_P(value)) {
	case IS_NULL: return true;
	case IS_FALSE: return strcmp(key, "ssl_ca_directory") == 0;
	case IS_STRING: return Z_STRLEN_P(value) == 0;
	case IS_ARRAY: return zend_hash_num_elements(Z_ARRVAL_P(value)) == 0;
	default: return false;
	}
}

static int ch_config_long(HashTable *config, const char *key, zend_long maximum,
	zend_long *out)
{
	zval *value = ch_config_value(config, key);
	zend_long result;
	if (!value) return SUCCESS;
	result = zval_get_long(value);
	if (EG(exception)) return FAILURE;
	if (result < 0 || result > maximum) {
		zend_throw_exception_ex(clickhouse_exception_ce, 0, "%s out of range", key);
		return FAILURE;
	}
	*out = result;
	return SUCCESS;
}

static int ch_config_timeout_ms(HashTable *config, const char *key,
	uint32_t maximum, uint32_t *out)
{
	zval *value = ch_config_value(config, key);
	uint32_t result;
	if (!value) return SUCCESS;
	if (Z_TYPE_P(value) == IS_DOUBLE) {
		double number = Z_DVAL_P(value);
		if (!isfinite(number) || number < 0.0 || number != trunc(number) || number > (double)maximum) {
			zend_throw_exception_ex(clickhouse_exception_ce, 0, "%s out of range", key);
			return FAILURE;
		}
		result = (uint32_t)number;
	} else {
		zend_long number = zval_get_long(value);
		if (EG(exception)) return FAILURE;
		if (number < 0 || (uint64_t)number > maximum) {
			zend_throw_exception_ex(clickhouse_exception_ce, 0, "%s out of range", key);
			return FAILURE;
		}
		result = (uint32_t)number;
	}
	*out = result;
	return SUCCESS;
}

static int ch_validate_database(zend_string *database)
{
	if (ZSTR_LEN(database) == 0) {
		return ch_config_error("database name must not be empty");
	}
	if (memchr(ZSTR_VAL(database), '.', ZSTR_LEN(database))) {
		return ch_config_error("database name contains an invalid character");
	}
	return SUCCESS;
}

/* Retained strings back every borrowed option until the backend clones it. */
static int ch_config_string(HashTable *strings, zval *value, ch_backend_string *out,
	const char *nul_error)
{
	zend_string *string = zval_get_string(value);
	zval owned;
	if (EG(exception)) {
		zend_string_release(string);
		return FAILURE;
	}
	if (nul_error && memchr(ZSTR_VAL(string), '\0', ZSTR_LEN(string))) {
		zend_string_release(string);
		return ch_config_error(nul_error);
	}
	out->data = ZSTR_VAL(string);
	out->len = ZSTR_LEN(string);
	ZVAL_STR(&owned, string);
	zend_hash_next_index_insert(strings, &owned);
	return SUCCESS;
}

static int ch_config_compression(zval *value, zend_long *out)
{
	switch (Z_TYPE_P(value)) {
	case IS_STRING: {
		const char *s = Z_STRVAL_P(value);
		size_t n = Z_STRLEN_P(value);
		if (n == 3 && strncasecmp(s, "lz4", n) == 0) *out = 1;
		else if (n == 4 && strncasecmp(s, "zstd", n) == 0) *out = 2;
		else if (n == 4 && strncasecmp(s, "none", n) == 0) *out = 0;
		else return ch_config_error("Unknown compression name; expected 'lz4', 'zstd', 'none', 0, 1, 2, true, or false");
		return SUCCESS;
	}
	case IS_TRUE: *out = 1; return SUCCESS;
	case IS_FALSE: *out = 0; return SUCCESS;
	case IS_LONG:
		if (Z_LVAL_P(value) < 0 || Z_LVAL_P(value) > 2) break;
		*out = Z_LVAL_P(value);
		return SUCCESS;
	case IS_DOUBLE:
		if (Z_DVAL_P(value) != 0.0 && Z_DVAL_P(value) != 1.0 && Z_DVAL_P(value) != 2.0) break;
		*out = (zend_long)Z_DVAL_P(value);
		return SUCCESS;
	default:
		return ch_config_error("compression must be 'lz4', 'zstd', 'none', 0, 1, 2, true, or false");
	}
	return ch_config_error("compression out of range; expected 0 (none), 1 (lz4), or 2 (zstd)");
}

static int ch_config_tls(HashTable *config, HashTable *strings,
	ch_backend_options *options, ch_backend_string **files_out)
{
	zend_string *key;
	zend_string *offending = NULL;
	zval *value;
	if (ch_load_config_bool(config, "ssl", &options->ssl) == FAILURE) return FAILURE;
#ifdef WITH_OPENSSL
	value = ch_config_value(config, "ssl_min_protocol_version");
	if (value) {
		static const char *versions[] = { "tls1.0", "tls1.1", "tls1.2", "tls1.3" };
		ch_backend_string version;
		size_t i;
		if (ch_config_string(strings, value, &version, NULL) == FAILURE) return FAILURE;
		options->ssl_min_protocol_version = 0;
		for (i = 0; i < sizeof(versions) / sizeof(versions[0]); i++) {
			if (version.len == 6 && strncasecmp(version.data, versions[i], version.len) == 0) {
				options->ssl_min_protocol_version = 0x0301 + (int)i;
				break;
			}
		}
		if (!options->ssl_min_protocol_version) {
			return ch_config_error("ssl_min_protocol_version must be one of tls1.0, tls1.1, tls1.2, tls1.3");
		}
	}
	if (ch_load_config_bool(config, "ssl_skip_verify", &options->ssl_skip_verify) == FAILURE ||
		ch_load_config_bool(config, "ssl_use_default_ca", &options->ssl_use_default_ca) == FAILURE) return FAILURE;
	value = ch_config_value(config, "ssl_ca_directory");
	if (value && Z_TYPE_P(value) != IS_FALSE) {
		if (Z_TYPE_P(value) != IS_STRING && Z_TYPE_P(value) != IS_OBJECT) {
			return ch_config_error("ssl_ca_directory must be a string");
		}
		if (ch_config_string(strings, value, &options->ssl_ca_directory,
			"ssl_ca_directory must not contain NUL bytes") == FAILURE) return FAILURE;
	}
	value = ch_config_value(config, "ssl_ca_files");
	if (value) {
		if (Z_TYPE_P(value) == IS_STRING) {
			*files_out = ecalloc(1, sizeof(**files_out));
			options->ssl_ca_files = *files_out;
			options->ssl_ca_file_count = 1;
			if (ch_config_string(strings, value, *files_out,
				"ssl_ca_files must not contain NUL bytes") == FAILURE) return FAILURE;
		} else if (Z_TYPE_P(value) == IS_ARRAY) {
			HashTable files;
			zval *file;
			size_t i = 0;
			int status = SUCCESS;
			ch_snapshot_array(&files, Z_ARRVAL_P(value));
			options->ssl_ca_file_count = zend_hash_num_elements(&files);
			if (options->ssl_ca_file_count) {
				*files_out = safe_emalloc(options->ssl_ca_file_count, sizeof(**files_out), 0);
				memset(*files_out, 0, options->ssl_ca_file_count * sizeof(**files_out));
			}
			options->ssl_ca_files = *files_out;
			ZEND_HASH_FOREACH_VAL(&files, file) {
				if (Z_TYPE_P(file) != IS_STRING && Z_TYPE_P(file) != IS_OBJECT) {
					status = ch_config_error("ssl_ca_files must be a string or an array of strings");
					break;
				}
				if (ch_config_string(strings, file, &(*files_out)[i++],
					"ssl_ca_files must not contain NUL bytes") == FAILURE) {
					status = FAILURE;
					break;
				}
			} ZEND_HASH_FOREACH_END();
			zend_hash_destroy(&files);
			if (status == FAILURE) return FAILURE;
		} else {
			return ch_config_error("ssl_ca_files must be a string or an array of strings");
		}
	}
	ZEND_HASH_FOREACH_STR_KEY(config, key) {
		static const char *known[] = {
			"ssl", "ssl_min_protocol_version", "ssl_skip_verify",
			"ssl_use_default_ca", "ssl_ca_directory", "ssl_ca_files"
		};
		size_t i;
		bool found = false;
		if (!key || ZSTR_LEN(key) < 3 || memcmp(ZSTR_VAL(key), "ssl", 3) != 0) continue;
		for (i = 0; i < sizeof(known) / sizeof(known[0]); i++) {
			if (ZSTR_LEN(key) == strlen(known[i]) && memcmp(ZSTR_VAL(key), known[i], ZSTR_LEN(key)) == 0) {
				found = true;
				break;
			}
		}
		if (!found) offending = key;
	} ZEND_HASH_FOREACH_END();
	if (offending) {
		zend_throw_exception_ex(clickhouse_exception_ce, 0, "unknown ssl_* option '%s'", ZSTR_VAL(offending));
		return FAILURE;
	}
	if (!options->ssl) {
		static const char *material[] = { "ssl_min_protocol_version", "ssl_ca_directory", "ssl_ca_files" };
		size_t i;
		for (i = 0; i < sizeof(material) / sizeof(material[0]); i++) {
			value = ch_config_value(config, material[i]);
			if (!value || (i != 0 && ch_unset_tls_path(material[i], value))) continue;
			zend_throw_exception_ex(clickhouse_exception_ce, 0,
				"ssl_* option '%s' requires 'ssl' => true", material[i]);
			return FAILURE;
		}
		if (options->ssl_skip_verify || !options->ssl_use_default_ca) {
			zend_throw_exception_ex(clickhouse_exception_ce, 0,
				"ssl_* option '%s' requires 'ssl' => true",
				options->ssl_skip_verify ? "ssl_skip_verify" : "ssl_use_default_ca");
			return FAILURE;
		}
	}
#else
	(void)strings;
	(void)files_out;
	if (options->ssl) {
		return ch_config_error("php_clickhouse was built without TLS support. Reconfigure with --enable-clickhouse-openssl");
	}
	ZEND_HASH_FOREACH_STR_KEY_VAL(config, key, value) {
		bool setting;
		bool is_boolean = false;
		bool default_value = false;
		if (!key || ZSTR_LEN(key) < 4 || memcmp(ZSTR_VAL(key), "ssl_", 4) != 0 || Z_TYPE_P(value) == IS_NULL) continue;
		if (zend_string_equals_literal(key, "ssl_skip_verify")) {
			is_boolean = true;
		} else if (zend_string_equals_literal(key, "ssl_use_default_ca")) {
			is_boolean = true;
			default_value = true;
		}
		if (is_boolean) {
			setting = default_value;
			if (ch_load_config_bool(config, ZSTR_VAL(key), &setting) == FAILURE) return FAILURE;
			if (setting == default_value) continue;
		} else if ((zend_string_equals_literal(key, "ssl_ca_files") ||
			zend_string_equals_literal(key, "ssl_ca_directory")) && ch_unset_tls_path(ZSTR_VAL(key), value)) {
			continue;
		}
		offending = key;
	} ZEND_HASH_FOREACH_END();
	if (offending) {
		zend_throw_exception_ex(clickhouse_exception_ce, 0,
			"ssl_* option '%s' requires php_clickhouse built with TLS support (--enable-clickhouse-openssl)",
			ZSTR_VAL(offending));
		return FAILURE;
	}
#endif
	return SUCCESS;
}

static int ch_config_endpoints(HashTable *config, HashTable *strings,
	ch_backend_options *options, ch_backend_endpoint **endpoints_out,
	ch_backend_string primary_host, uint16_t primary_port, bool host_configured)
{
	zval *value = ch_config_value(config, "endpoints");
	HashTable endpoints;
	zval *entry;
	size_t count = 0;
	int status = SUCCESS;
	if (!value) {
		*endpoints_out = ecalloc(1, sizeof(**endpoints_out));
		(*endpoints_out)[0].host = primary_host;
		(*endpoints_out)[0].port = primary_port;
		options->endpoints = *endpoints_out;
		options->endpoint_count = primary_host.len ? 1 : 0;
		return SUCCESS;
	}
	if (Z_TYPE_P(value) != IS_ARRAY) {
		return ch_config_error("endpoints must be a list of [host, port] arrays");
	}
	ch_snapshot_array(&endpoints, Z_ARRVAL_P(value));
	if (!zend_hash_num_elements(&endpoints)) {
		zend_hash_destroy(&endpoints);
		return ch_config_error("endpoints was provided but contained no usable entries");
	}
	*endpoints_out = safe_emalloc(zend_hash_num_elements(&endpoints), sizeof(**endpoints_out), sizeof(**endpoints_out));
	options->endpoints = *endpoints_out;
	if (host_configured && primary_host.len) {
		(*endpoints_out)[count].host = primary_host;
		(*endpoints_out)[count++].port = primary_port;
	}
	ZEND_HASH_FOREACH_VAL(&endpoints, entry) {
		zval host;
		zval port;
		zval *source;
		zend_long port_number = 9000;
		ch_backend_endpoint *endpoint = &(*endpoints_out)[count];
		if (Z_TYPE_P(entry) != IS_ARRAY) {
			status = ch_config_error("each endpoints entry must be an array with a 'host' key");
			break;
		}
		source = ch_config_value(Z_ARRVAL_P(entry), "host");
		if (!source) {
			status = ch_config_error("each endpoints entry requires a non-null 'host'");
			break;
		}
		ZVAL_COPY_DEREF(&host, source);
		source = zend_hash_str_find(Z_ARRVAL_P(entry), "port", sizeof("port") - 1);
		if (source) ZVAL_COPY_DEREF(&port, source);
		else ZVAL_UNDEF(&port);
		status = ch_config_string(strings, &host, &endpoint->host, "host must not contain NUL bytes");
		if (status == SUCCESS && !Z_ISUNDEF(port)) {
			port_number = zval_get_long(&port);
			if (EG(exception)) status = FAILURE;
			else if (port_number < 1 || port_number > 65535) {
				status = ch_config_error("Endpoint port out of 1..65535 range");
			}
		}
		zval_ptr_dtor(&host);
		if (!Z_ISUNDEF(port)) zval_ptr_dtor(&port);
		if (status == FAILURE) break;
		endpoint->port = (uint16_t)port_number;
		count++;
	} ZEND_HASH_FOREACH_END();
	zend_hash_destroy(&endpoints);
	options->endpoint_count = count;
	return status;
}

static zend_long ch_property_long(zval *object, const char *name)
{
	zval temporary;
	zval *value = sc_zend_read_property(clickhouse_ce, object, name, strlen(name), 0, &temporary);
	return Z_LVAL_P(value);
}

static int ch_config_property_long(HashTable *config, zval *object,
	const char *name, zend_long maximum, zend_long *out)
{
	*out = ch_property_long(object, name);
	if (ch_config_long(config, name, maximum, out) == FAILURE) return FAILURE;
	if (ch_config_value(config, name)) {
		sc_zend_update_property_long(clickhouse_ce, object, name, strlen(name), *out);
	}
	return EG(exception) ? FAILURE : SUCCESS;
}

static int ch_config_options(HashTable *config, HashTable *strings,
	ch_backend_options *options, ch_backend_endpoint **endpoints,
	ch_backend_string **ca_files, zval *object)
{
	zval *value;
	zval temporary;
	ch_backend_string host;
	zend_long number;
	zend_long port;
	bool host_configured;
	ch_backend_options_init(options);
	value = ch_config_value(config, "host");
	host_configured = value != NULL;
	if (!value) value = sc_zend_read_property(clickhouse_ce, object, "host", sizeof("host") - 1, 0, &temporary);
	if (ch_config_string(strings, value, &host, "host must not contain NUL bytes") == FAILURE) return FAILURE;
	if (host_configured) {
		sc_zend_update_property_stringl(clickhouse_ce, object, "host", sizeof("host") - 1, host.data, host.len);
		if (EG(exception)) return FAILURE;
	}
	port = ch_property_long(object, "port");
	value = ch_config_value(config, "port");
	if (value) {
		port = zval_get_long(value);
		if (EG(exception)) return FAILURE;
		if (port < 1 || port > 65535) return ch_config_error("port out of 1..65535 range");
		sc_zend_update_property_long(clickhouse_ce, object, "port", sizeof("port") - 1, port);
	}
	number = ch_property_long(object, "compression");
	value = ch_config_value(config, "compression");
	if (value) {
		if (ch_config_compression(value, &number) == FAILURE) return FAILURE;
		sc_zend_update_property_long(clickhouse_ce, object, "compression", sizeof("compression") - 1, number);
	}
	options->compression = (chc_compression)number;
	if (ch_config_property_long(config, object, "retry_timeout", ZEND_LONG_MAX, &number) == FAILURE) return FAILURE;
	options->retry_timeout_seconds = (uint64_t)number;
	if (ch_config_property_long(config, object, "retry_count",
		(uint64_t)UINT_MAX > (uint64_t)ZEND_LONG_MAX ? ZEND_LONG_MAX : (zend_long)UINT_MAX,
		&number) == FAILURE) return FAILURE;
	options->retry_count = (uint32_t)number;
	if (ch_config_property_long(config, object, "connect_timeout", INT_MAX / 1000, &number) == FAILURE) return FAILURE;
	options->connect_timeout_ms = (uint32_t)number * 1000U;
	if (ch_config_property_long(config, object, "receive_timeout", UINT_MAX / 1000U, &number) == FAILURE) return FAILURE;
	options->receive_timeout_ms = (uint32_t)number * 1000U;
	number = 0;
	if (ch_config_long(config, "send_timeout", UINT_MAX / 1000U, &number) == FAILURE) return FAILURE;
	options->send_timeout_ms = (uint32_t)number * 1000U;
	if (ch_config_timeout_ms(config, "connect_timeout_ms", INT_MAX, &options->connect_timeout_ms) == FAILURE ||
		ch_config_timeout_ms(config, "receive_timeout_ms", UINT32_MAX, &options->receive_timeout_ms) == FAILURE ||
		ch_config_timeout_ms(config, "send_timeout_ms", UINT32_MAX, &options->send_timeout_ms) == FAILURE) return FAILURE;
	if (ch_load_config_bool(config, "tcp_nodelay", &options->tcp_nodelay) == FAILURE ||
		ch_load_config_bool(config, "tcp_keepalive", &options->tcp_keepalive) == FAILURE ||
		ch_load_config_bool(config, "ping_before_query", &options->ping_before_query) == FAILURE) return FAILURE;
	number = options->keepalive_idle_seconds;
	if (ch_config_long(config, "tcp_keepalive_idle", INT_MAX, &number) == FAILURE) return FAILURE;
	options->keepalive_idle_seconds = (uint32_t)number;
	number = options->keepalive_interval_seconds;
	if (ch_config_long(config, "tcp_keepalive_intvl", INT_MAX, &number) == FAILURE) return FAILURE;
	options->keepalive_interval_seconds = (uint32_t)number;
	number = options->keepalive_count;
	if (ch_config_long(config, "tcp_keepalive_cnt", INT_MAX, &number) == FAILURE) return FAILURE;
	options->keepalive_count = (uint32_t)number;
	number = options->max_compression_chunk_size;
	if (ch_config_long(config, "max_compression_chunk_size", INT_MAX, &number) == FAILURE) return FAILURE;
	options->max_compression_chunk_size = (uint32_t)number;
	if (ch_config_tls(config, strings, options, ca_files) == FAILURE) return FAILURE;
	if (ch_config_endpoints(config, strings, options, endpoints, host, (uint16_t)port, host_configured) == FAILURE) return FAILURE;
	value = ch_config_value(config, "database");
	if (value) {
		if (ch_config_string(strings, value, &options->database, NULL) == FAILURE) return FAILURE;
		if (!options->database.len) return ch_config_error("database name must not be empty");
		if (memchr(options->database.data, '.', options->database.len)) return ch_config_error("database name contains an invalid character");
		sc_zend_update_property_stringl(clickhouse_ce, object, "database", sizeof("database") - 1,
			options->database.data, options->database.len);
		if (EG(exception)) return FAILURE;
	}
	value = ch_config_value(config, "user");
	if (value) {
		if (ch_config_string(strings, value, &options->user, NULL) == FAILURE) return FAILURE;
		sc_zend_update_property_stringl(clickhouse_ce, object, "user", sizeof("user") - 1,
			options->user.data, options->user.len);
		if (EG(exception)) return FAILURE;
	}
	value = ch_config_value(config, "passwd");
	if (!value) value = ch_config_value(config, "password");
	if (value && ch_config_string(strings, value, &options->password, NULL) == FAILURE) return FAILURE;
	return SUCCESS;
}

PHP_METHOD(ClickHouse, __construct)
{
	zval *config_input;
	clickhouse_object *obj;
	HashTable config;
	HashTable strings;
	ch_backend_options options;
	ch_backend_endpoint *endpoints = NULL;
	ch_backend_string *ca_files = NULL;
	chc_exception *server_error = NULL;
	chc_err error = {0};
	int status;
	ZEND_PARSE_PARAMETERS_START(1, 1)
		Z_PARAM_ARRAY(config_input)
	ZEND_PARSE_PARAMETERS_END();
	obj = Z_CLICKHOUSE_P(getThis());
	if (obj->backend || obj->pending_backend || obj->query_active) {
		ch_config_error("ClickHouse object is already constructed");
		return;
	}
	obj->query_active = true;
	ch_snapshot_array(&config, Z_ARRVAL_P(config_input));
	zend_hash_init(&strings, 0, NULL, ZVAL_PTR_DTOR, 0);
	if (ch_config_options(&config, &strings, &options, &endpoints, &ca_files, getThis()) == FAILURE) goto cleanup;
	status = ch_backend_create(&obj->pending_backend, &options, &ch_php_allocator, &error);
	if (status == CHC_OK) status = ch_backend_connect(obj->pending_backend, &server_error, &error);
	if (status != CHC_OK) {
		ch_throw_error(status, &error, server_error, NULL);
		goto cleanup;
	}
	obj->backend = obj->pending_backend;
	obj->pending_backend = NULL;
	RETVAL_TRUE;
cleanup:
	if (server_error) chc_exception_free(server_error, &ch_php_allocator);
	ch_backend_destroy(obj->pending_backend);
	obj->pending_backend = NULL;
	if (endpoints) efree(endpoints);
	if (ca_files) efree(ca_files);
	zend_hash_destroy(&strings);
	zend_hash_destroy(&config);
	obj->query_active = false;
}

PHP_METHOD(ClickHouse, __destruct)
{
}

PHP_METHOD(ClickHouse, ping)
{
	clickhouse_object *obj;
	chc_exception *server_error = NULL;
	chc_err error = {0};
	int status;
	if (zend_parse_parameters_none() == FAILURE) return;
	obj = Z_CLICKHOUSE_P(getThis());
	if (ch_operation_begin(obj, false) == FAILURE) return;
	status = ch_backend_ping(obj->backend, &server_error, &error);
	if (status != CHC_OK) ch_throw_error(status, &error, server_error, NULL);
	if (server_error) chc_exception_free(server_error, &ch_php_allocator);
	ch_operation_end(obj);
	if (!EG(exception)) RETVAL_TRUE;
}

PHP_METHOD(ClickHouse, setSettings)
{
	zval *settings;
	HashTable snapshot;
	HashTable converted;
	zend_string *key;
	zval *value;
	clickhouse_object *obj;
	bool success = false;
	ZEND_PARSE_PARAMETERS_START(1, 1)
		Z_PARAM_ARRAY(settings)
	ZEND_PARSE_PARAMETERS_END();
	obj = Z_CLICKHOUSE_P(getThis());
	ch_snapshot_array(&snapshot, Z_ARRVAL_P(settings));
	zend_hash_init(&converted, zend_hash_num_elements(&snapshot), NULL, ZVAL_PTR_DTOR, 0);
	ZEND_HASH_FOREACH_STR_KEY_VAL(&snapshot, key, value) {
		zend_string *string = NULL;
		zval item;
		if (!key) {
			ch_config_error("setting keys must be strings");
			goto cleanup;
		}
		if (!ZSTR_LEN(key)) {
			ch_config_error("setting key must not be empty");
			goto cleanup;
		}
		if (ch_format_scalar(&string, value) == FAILURE) goto cleanup;
		ZVAL_STR(&item, string);
		zend_hash_update(&converted, key, &item);
	} ZEND_HASH_FOREACH_END();
	zend_hash_destroy(&obj->settings);
	obj->settings = converted;
	success = true;
cleanup:
	if (!success) zend_hash_destroy(&converted);
	zend_hash_destroy(&snapshot);
	if (!EG(exception)) RETVAL_ZVAL(getThis(), 1, 0);
}

PHP_METHOD(ClickHouse, setSetting)
{
	zend_string *key;
	zend_string *string = NULL;
	zval *value;
	zval copied;
	zval item;
	clickhouse_object *obj;
	int status;
	ZEND_PARSE_PARAMETERS_START(2, 2)
		Z_PARAM_STR(key)
		Z_PARAM_ZVAL(value)
	ZEND_PARSE_PARAMETERS_END();
	if (!ZSTR_LEN(key)) {
		ch_config_error("setting key must not be empty");
		return;
	}
	obj = Z_CLICKHOUSE_P(getThis());
	key = zend_string_copy(key);
	ZVAL_COPY_DEREF(&copied, value);
	status = ch_format_scalar(&string, &copied);
	zval_ptr_dtor(&copied);
	if (status == SUCCESS && !EG(exception)) {
		ZVAL_STR(&item, string);
		zend_hash_update(&obj->settings, key, &item);
		RETVAL_ZVAL(getThis(), 1, 0);
	} else if (string) {
		zend_string_release(string);
	}
	zend_string_release(key);
}

PHP_METHOD(ClickHouse, setDatabase)
{
	zend_string *database;
	clickhouse_object *obj;
	chc_exception *server_error = NULL;
	chc_err error = {0};
	int status;
	ZEND_PARSE_PARAMETERS_START(1, 1)
		Z_PARAM_STR(database)
	ZEND_PARSE_PARAMETERS_END();
	if (!ZSTR_LEN(database)) {
		ch_config_error("database name must not be empty");
		return;
	}
	obj = Z_CLICKHOUSE_P(getThis());
	if (!obj->backend) {
		ch_config_error("Client is not connected");
		return;
	}
	if (ch_operation_begin(obj, false) == FAILURE) return;
	if (ch_validate_database(database) == FAILURE) goto cleanup;
	status = ch_backend_clone_database(&obj->pending_backend, obj->backend,
		ZSTR_VAL(database), ZSTR_LEN(database), &error);
	if (status == CHC_OK) status = ch_backend_connect(obj->pending_backend, &server_error, &error);
	if (status != CHC_OK) {
		ch_throw_error(status, &error, server_error, NULL);
		goto cleanup;
	}
	ch_backend_destroy(obj->backend);
	obj->backend = obj->pending_backend;
	obj->pending_backend = NULL;
	sc_zend_update_property_stringl(clickhouse_ce, getThis(), "database", sizeof("database") - 1,
		ZSTR_VAL(database), ZSTR_LEN(database));
cleanup:
	if (server_error) chc_exception_free(server_error, &ch_php_allocator);
	ch_backend_destroy(obj->pending_backend);
	obj->pending_backend = NULL;
	ch_operation_end(obj);
	if (!EG(exception)) RETVAL_ZVAL(getThis(), 1, 0);
}

static int ch_set_callback(zval *target, zval *callback, const char *name)
{
	zval replacement = {0};
	zval old;
	ZVAL_DEREF(callback);
	if (Z_TYPE_P(callback) != IS_NULL && !zend_is_callable(callback, 0, NULL)) {
		if (!EG(exception)) {
			zend_throw_exception_ex(clickhouse_exception_ce, 0, "%s expects a callable or null", name);
		}
		return FAILURE;
	}
	if (EG(exception)) return FAILURE;
	if (Z_TYPE_P(callback) == IS_NULL) ZVAL_UNDEF(&replacement);
	else ZVAL_COPY(&replacement, callback);
	ZVAL_COPY_VALUE(&old, target);
	ZVAL_COPY_VALUE(target, &replacement);
	if (!Z_ISUNDEF(old)) zval_ptr_dtor(&old);
	return EG(exception) ? FAILURE : SUCCESS;
}

PHP_METHOD(ClickHouse, setProgressCallback)
{
	zval *callback;
	ZEND_PARSE_PARAMETERS_START(1, 1)
		Z_PARAM_ZVAL(callback)
	ZEND_PARSE_PARAMETERS_END();
	if (ch_set_callback(&Z_CLICKHOUSE_P(getThis())->progress_callback, callback, "setProgressCallback") == FAILURE) return;
	RETURN_TRUE;
}

PHP_METHOD(ClickHouse, setProfileCallback)
{
	zval *callback;
	ZEND_PARSE_PARAMETERS_START(1, 1)
		Z_PARAM_ZVAL(callback)
	ZEND_PARSE_PARAMETERS_END();
	if (ch_set_callback(&Z_CLICKHOUSE_P(getThis())->profile_callback, callback, "setProfileCallback") == FAILURE) return;
	RETURN_TRUE;
}

PHP_METHOD(ClickHouse, setVerbose)
{
	zval *sink;
	zval replacement = {0};
	zval old;
	bool to_stderr = false;
	clickhouse_object *obj;
	ZEND_PARSE_PARAMETERS_START(1, 1)
		Z_PARAM_ZVAL(sink)
	ZEND_PARSE_PARAMETERS_END();
	ZVAL_DEREF(sink);
	ZVAL_UNDEF(&replacement);
	if (Z_TYPE_P(sink) == IS_TRUE) {
		to_stderr = true;
	} else if (Z_TYPE_P(sink) != IS_FALSE && Z_TYPE_P(sink) != IS_NULL) {
		if (!zend_is_callable(sink, 0, NULL)) {
			ch_config_error("setVerbose expects bool, null, or callable");
			return;
		}
		if (EG(exception)) return;
		ZVAL_COPY(&replacement, sink);
	}
	obj = Z_CLICKHOUSE_P(getThis());
	ZVAL_COPY_VALUE(&old, &obj->verbose_callback);
	ZVAL_COPY_VALUE(&obj->verbose_callback, &replacement);
	obj->verbose_to_stderr = to_stderr;
	if (!Z_ISUNDEF(old)) zval_ptr_dtor(&old);
	if (!EG(exception)) RETVAL_ZVAL(getThis(), 1, 0);
}

PHP_METHOD(ClickHouse, resetConnection)
{
	clickhouse_object *obj;
	chc_exception *server_error = NULL;
	chc_err error = {0};
	int status;
	if (zend_parse_parameters_none() == FAILURE) return;
	obj = Z_CLICKHOUSE_P(getThis());
	if (ch_operation_begin(obj, true) == FAILURE) return;
	ch_insert_clear(obj);
	status = ch_backend_reset(obj->backend, &server_error, &error);
	if (status != CHC_OK) ch_throw_error(status, &error, server_error, NULL);
	if (server_error) chc_exception_free(server_error, &ch_php_allocator);
	ch_operation_end(obj);
	if (!EG(exception)) RETVAL_TRUE;
}

PHP_METHOD(ClickHouse, getServerInfo)
{
	clickhouse_object *obj;
	const chc_server_info *info;
	if (zend_parse_parameters_none() == FAILURE) return;
	obj = Z_CLICKHOUSE_P(getThis());
	if (!obj->backend) {
		ch_config_error("ClickHouse client is not initialized");
		return;
	}
	info = ch_backend_server_info(obj->backend);
	if (!info) {
		ch_config_error("Client is not connected");
		return;
	}
	array_init(return_value);
	add_assoc_stringl(return_value, "name", info->full_name ? info->full_name : "", info->full_name_len);
	add_assoc_stringl(return_value, "display_name", info->full_display_name ? info->full_display_name : "", info->full_display_name_len);
	add_assoc_long(return_value, "version_major", (zend_long)info->version_major);
	add_assoc_long(return_value, "version_minor", (zend_long)info->version_minor);
	add_assoc_long(return_value, "version_patch", info->server_revision >= 54401 ? (zend_long)info->version_patch : 0);
	add_assoc_long(return_value, "revision", (zend_long)info->server_revision);
	add_assoc_stringl(return_value, "timezone", info->full_timezone ? info->full_timezone : "", info->full_timezone_len);
}

PHP_METHOD(ClickHouse, getCurrentEndpoint)
{
	clickhouse_object *obj;
	const ch_backend_endpoint *endpoint;
	if (zend_parse_parameters_none() == FAILURE) return;
	obj = Z_CLICKHOUSE_P(getThis());
	if (!obj->backend) {
		ch_config_error("ClickHouse client is not initialized");
		return;
	}
	endpoint = ch_backend_current_endpoint(obj->backend);
	if (!endpoint) RETURN_NULL();
	array_init(return_value);
	add_assoc_stringl(return_value, "host", (char *)endpoint->host.data, endpoint->host.len);
	add_assoc_long(return_value, "port", endpoint->port);
}

PHP_METHOD(ClickHouse, getStatistics)
{
	if (zend_parse_parameters_none() == FAILURE) return;
	ch_stats_array(return_value, &Z_CLICKHOUSE_P(getThis())->stats);
}

PHP_METHOD(ClickHouse, enableLogQueries)
{
	zend_bool enabled = 1;
	ZEND_PARSE_PARAMETERS_START(0, 1)
		Z_PARAM_OPTIONAL
		Z_PARAM_BOOL(enabled)
	ZEND_PARSE_PARAMETERS_END();
	Z_CLICKHOUSE_P(getThis())->log_enabled = enabled != 0;
	RETURN_TRUE;
}

PHP_METHOD(ClickHouse, getLogQueries)
{
	clickhouse_object *obj;
	zval *entry;
	if (zend_parse_parameters_none() == FAILURE) return;
	obj = Z_CLICKHOUSE_P(getThis());
	array_init_size(return_value, zend_hash_num_elements(&obj->query_log));
	ZEND_HASH_FOREACH_VAL(&obj->query_log, entry) {
		zval copy;
		ZVAL_COPY(&copy, entry);
		add_next_index_zval(return_value, &copy);
	} ZEND_HASH_FOREACH_END();
	zend_hash_clean(&obj->query_log);
	obj->query_log_next = 0;
}

PHP_METHOD(ClickHouseException, getServerCode)
{
	zval temporary;
	zval *property;
	if (zend_parse_parameters_none() == FAILURE) return;
	property = sc_zend_read_property(clickhouse_exception_ce, getThis(), "server_code", sizeof("server_code") - 1, 0, &temporary);
	if (property) ZVAL_DEREF(property);
	if (property && Z_TYPE_P(property) == IS_LONG) RETURN_LONG(Z_LVAL_P(property));
	RETURN_LONG(0);
}

PHP_METHOD(ClickHouseException, getServerName)
{
	zval temporary;
	zval *property;
	if (zend_parse_parameters_none() == FAILURE) return;
	property = sc_zend_read_property(clickhouse_exception_ce, getThis(), "server_name", sizeof("server_name") - 1, 0, &temporary);
	if (property) ZVAL_DEREF(property);
	if (property && Z_TYPE_P(property) == IS_STRING) RETURN_STR_COPY(Z_STR_P(property));
	RETURN_NULL();
}

PHP_METHOD(ClickHouseException, getQueryId)
{
	zval temporary;
	zval *property;
	if (zend_parse_parameters_none() == FAILURE) return;
	property = sc_zend_read_property(clickhouse_exception_ce, getThis(), "query_id", sizeof("query_id") - 1, 0, &temporary);
	if (property) ZVAL_DEREF(property);
	if (property && Z_TYPE_P(property) == IS_STRING) RETURN_STR_COPY(Z_STR_P(property));
	RETURN_NULL();
}
