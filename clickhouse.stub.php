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
    // 0=none, 1=lz4, 2=zstd. Was `bool` but that coerced 2 → true → 1
    // on read-back, silently downgrading "zstd" callers to LZ4.
    protected int $compression = 0;
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

    public function selectWithExternalData(
        string $sql,
        array $externals,
        array $params = [],
        int $fetch_mode = 0,
        string $query_id = "",
        array $settings = []
    ): mixed {}

    public function selectToStream(
        string $sql,
        array $params,
        mixed $stream,
        string $format = "TabSeparated",
        string $query_id = "",
        array $settings = []
    ): int {}

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

    public function insertFromStream(
        string $table,
        array $columns,
        mixed $stream,
        string $format = "TabSeparated",
        int $batch_rows = 10000,
        string $query_id = "",
        array $settings = []
    ): int {}

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

    public function setSettings(array $settings): static {}

    public function setSetting(string $key, mixed $value): static {}

    public function setDatabase(string $database): static {}

    public function setProgressCallback(?callable $callback): bool {}

    public function setProfileCallback(?callable $callback): bool {}

    public function setVerbose(mixed $sink): static {}

    public function resetConnection(): bool {}

    public function getServerInfo(): array {}

    public function getCurrentEndpoint(): ?array {}

    public function getStatistics(): array {}

    public function databaseSize(?string $database = null): array {}

    public function tablesSize(?string $database = null): array {}

    public function partitions(string $table): array {}

    public function showTables(?string $database = null, ?string $like = null): array {}

    public function showCreateTable(string $table): string {}

    public function getServerUptime(): int {}

    public function enableLogQueries(bool $enabled = true): bool {}

    public function getLogQueries(): array {}

    public function selectStream(
        string $sql,
        array $params = [],
        string $query_id = "",
        array $settings = []
    ): ClickHouseRowIterator {}

    public function selectStatement(
        string $sql,
        array $params = [],
        string $query_id = "",
        array $settings = []
    ): ClickHouseStatement {}

    public function selectStreamCallback(
        string $sql,
        callable $callback,
        array $params = [],
        string $query_id = "",
        array $settings = []
    ): bool {}

    public function isExists(string $database, string $table): bool {}

    public function showDatabases(): array {}

    public function showProcesslist(): array {}

    public function getServerVersion(): string {}

    public function tableSize(string $table): array {}

    public function truncateTable(string $table): bool {}

    public function dropPartition(string $table, string $partition): bool {}
}

final class ClickHouseRowIterator implements Iterator, Countable
{
    public function rewind(): void {}

    public function valid(): bool {}

    public function current(): array {}

    public function key(): int {}

    public function next(): void {}

    public function count(): int {}
}

final class ClickHouseStatement implements Iterator, Countable, ArrayAccess, JsonSerializable
{
    private function __construct() {}

    public function count(): int {}

    public function rewind(): void {}

    public function valid(): bool {}

    public function current(): mixed {}

    public function key(): mixed {}

    public function next(): void {}

    public function offsetExists(mixed $offset): bool {}

    public function offsetGet(mixed $offset): mixed {}

    public function offsetSet(mixed $offset, mixed $value): void {}

    public function offsetUnset(mixed $offset): void {}

    public function jsonSerialize(): array {}

    public function toArray(): array {}

    public function statistics(): array {}

    public function fetchOne(): mixed {}

    public function fetchKeyPair(): array {}

    public function fetchColumn(): array {}
}

class ClickHouseException extends Exception
{
    public int $server_code = 0;
    public ?string $server_name = null;
    public ?string $query_id = null;

    public function getServerCode(): int {}

    public function getServerName(): ?string {}

    public function getQueryId(): ?string {}
}
