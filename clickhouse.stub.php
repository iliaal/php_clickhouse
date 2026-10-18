<?php

/** @generate-class-entries */

/** Legacy class-name aliases for this class and ClickHouseException stay
 * registered for source compatibility; using them raises no runtime
 * deprecation at this time. */
final class ClickHouse
{
    public const int FETCH_ONE = 1;
    public const int FETCH_KEY_PAIR = 2;
    public const int DATE_AS_STRINGS = 4;
    public const int FETCH_COLUMN = 8;
    public const int JSON_AS_ARRAY = 16;
    public const int JSON_AS_OBJECT = 32;
    public const int UUID_WITH_DASHES = 64;
    public const int FIXEDSTRING_BINARY = 128;
    public const int MAP_AS_PAIRS = 256;

    protected string $host = "127.0.0.1";
    protected int $port = 9000;
    protected string $database = "default";
    protected ?string $user = null;
    // Keep passwords out of properties to prevent introspection leaks.
    // 0=none, 1=lz4, 2=zstd; bool would silently downgrade zstd to lz4.
    protected int $compression = 0;
    protected int $retry_timeout = 5;
    protected int $retry_count = 1;
    protected int $receive_timeout = 0;
    protected int $connect_timeout = 5;

    public function __construct(#[\SensitiveParameter] array $connectParams) {}

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

    /**
     * Write query rows to $stream as TSV/CSV. $params is required (pass []
     * when the query has no placeholders). FixedString cells are emitted
     * with trailing NUL padding trimmed; use select() with
     * FIXEDSTRING_BINARY for binary-exact reads.
     */
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

    /**
     * Enable protocol-level lifecycle tracing: true logs JSON lines on
     * STDERR, false or null disables, a callable receives each event.
     * Chainable.
     */
    public function setVerbose(bool|callable|null $sink): static {}

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

    /**
     * Lazy row iterator. Only the value-shaping fetch flags apply
     * (DATE_AS_STRINGS, JSON_AS_ARRAY / JSON_AS_OBJECT, UUID_WITH_DASHES,
     * FIXEDSTRING_BINARY, MAP_AS_PAIRS); the row-shape flags (FETCH_ONE /
     * FETCH_KEY_PAIR / FETCH_COLUMN) are ignored. $fetch_mode is the
     * trailing argument.
     */
    public function selectStream(
        string $sql,
        array $params = [],
        string $query_id = "",
        array $settings = [],
        int $fetch_mode = 0
    ): ClickHouseRowIterator {}

    /**
     * Buffered result wrapper. Only the value-shaping fetch flags apply
     * (DATE_AS_STRINGS, JSON_AS_ARRAY / JSON_AS_OBJECT, UUID_WITH_DASHES,
     * FIXEDSTRING_BINARY, MAP_AS_PAIRS); the row-shape flags (FETCH_ONE /
     * FETCH_KEY_PAIR / FETCH_COLUMN) are ignored since the statement always
     * carries full rows.
     */
    public function selectStatement(
        string $sql,
        array $params = [],
        string $query_id = "",
        array $settings = [],
        int $fetch_mode = 0
    ): ClickHouseStatement {}

    /**
     * Per-row streaming read. Only the value-shaping fetch flags apply
     * (DATE_AS_STRINGS, JSON_AS_ARRAY / JSON_AS_OBJECT, UUID_WITH_DASHES,
     * FIXEDSTRING_BINARY, MAP_AS_PAIRS); the row-shape flags (FETCH_ONE /
     * FETCH_KEY_PAIR / FETCH_COLUMN) are ignored. $fetch_mode is the
     * trailing argument.
     */
    public function selectStreamCallback(
        string $sql,
        callable $callback,
        array $params = [],
        string $query_id = "",
        array $settings = [],
        int $fetch_mode = 0
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

    public function current(): ?array {}

    public function key(): int {}

    public function next(): void {}

    public function count(): int {}
}

final class ClickHouseStatement implements Iterator, Countable, ArrayAccess, JsonSerializable
{
    /**
     * Not callable directly: always throws ClickHouseException. Obtain
     * instances from ClickHouse::selectStatement().
     */
    public function __construct() {}

    public function count(): int {}

    public function rewind(): void {}

    public function valid(): bool {}

    public function current(): mixed {}

    public function key(): mixed {}

    public function next(): void {}

    public function offsetExists(mixed $offset): bool {}

    public function offsetGet(mixed $offset): mixed {}

    /** @throws ClickHouseException always: statements are read-only. */
    public function offsetSet(mixed $offset, mixed $value): void {}

    /** @throws ClickHouseException always: statements are read-only. */
    public function offsetUnset(mixed $offset): void {}

    public function jsonSerialize(): array {}

    public function toArray(): array {}

    public function statistics(): array {}

    /**
     * First result row: null when the result is empty, the scalar cell when
     * the row holds a single column, otherwise the full assoc row.
     */
    public function fetchOne(): mixed {}

    /**
     * Column 0 => column 1 map over every row; [] when the result is empty.
     * Throws when any row holds fewer than 2 columns or the key column is
     * not scalar (array / object keys are rejected rather than collapsing
     * onto the string "Array").
     */
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
