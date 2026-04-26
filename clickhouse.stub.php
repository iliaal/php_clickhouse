<?php

/** @generate-class-entries */

final class ClickHouse
{
    public const int FETCH_ONE = 1;
    public const int FETCH_KEY_PAIR = 2;
    public const int DATE_AS_STRINGS = 4;
    public const int FETCH_COLUMN = 8;

    protected string $host = "127.0.0.1";
    protected int $port = 9000;
    protected string $database = "default";
    protected ?string $user = null;
    // No `passwd` property is declared. The secret stays out of
    // get_object_vars, var_dump, serialize, and reflection by simply
    // not being stored on the object.
    protected bool $compression = false;
    protected int $retry_timeout = 5;
    protected int $retry_count = 1;
    protected int $receive_timeout = 0;
    protected int $connect_timeout = 5;

    public function __construct(array $connectParams) {}

    public function __destruct() {}

    public function select(
        string $sql,
        array $params = [],
        int $fetch_mode = 0,
        string $query_id = "",
        array $settings = []
    ): mixed {}

    public function insert(
        string $table,
        array $columns,
        array $values,
        string $query_id = "",
        array $settings = []
    ): bool {}

    public function insertAssoc(
        string $table,
        array $rows,
        string $query_id = "",
        array $settings = []
    ): bool {}

    public function writeStart(
        string $table,
        array $columns,
        string $query_id = "",
        array $settings = []
    ): bool {}

    public function write(array $values): bool {}

    public function writeEnd(): bool {}

    public function execute(
        string $sql,
        array $params = [],
        string $query_id = "",
        array $settings = []
    ): bool {}

    public function ping(): bool {}

    public function setSettings(array $settings): bool {}

    public function setProgressCallback(?callable $callback): bool {}

    public function getStatistics(): array {}

    public function databaseSize(?string $database = null): array {}

    public function tablesSize(?string $database = null): array {}

    public function partitions(string $table): array {}

    public function showTables(?string $database = null, ?string $like = null): array {}

    public function showCreateTable(string $table): string {}

    public function getServerUptime(): int {}

    public function enableLogQueries(bool $enabled = true): bool {}

    public function getLogQueries(): array {}
}

class ClickHouseException extends Exception
{
    public int $server_code = 0;
    public ?string $server_name = null;
    public ?string $query_id = null;
}
