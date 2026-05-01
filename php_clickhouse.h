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
  | Original SeasClick author: SeasX Group <ahhhh.wang@gmail.com>        |
  +----------------------------------------------------------------------+
*/
#ifdef __cplusplus
#define __STDC_FORMAT_MACROS
#endif

#ifndef PHP_CLICKHOUSE_H
#define PHP_CLICKHOUSE_H

extern zend_module_entry clickhouse_module_entry;
#define phpext_clickhouse_ptr &clickhouse_module_entry

#define PHP_CLICKHOUSE_VERSION "0.8.1"

#ifdef PHP_WIN32
#	define PHP_CLICKHOUSE_API __declspec(dllexport)
#elif defined(__GNUC__) && __GNUC__ >= 4
#	define PHP_CLICKHOUSE_API __attribute__ ((visibility("default")))
#else
#	define PHP_CLICKHOUSE_API
#endif

#define SC_FETCH_ONE 1
#define SC_FETCH_KEY_PAIR 2
#define SC_FETCH_DATE_AS_STRINGS 4
#define SC_FETCH_COLUMN 8

#define CLICKHOUSE_RES_NAME "ClickHouse"
#define CLICKHOUSE_EXCEPTION_NAME "ClickHouseException"

/* Back-compat aliases for the original SeasClick name. Kept for the 0.5.x
 * release cycle and removed in the next major. */
#define CLICKHOUSE_RES_NAME_LEGACY "SeasClick"
#define CLICKHOUSE_EXCEPTION_NAME_LEGACY "SeasClickException"

#endif	/* PHP_CLICKHOUSE_H */

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
