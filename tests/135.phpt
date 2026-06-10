--TEST--
A profile callback that unregisters itself mid-call is not destroyed before it returns
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

/* The handler instance is referenced only by the callback array stored on
 * the client. When run() calls setProfileCallback(null), the stored array
 * is dtor'd; without pinning the callable across the call that drops the
 * last reference and destroys the handler whose method is still executing
 * (a use-after-free on $this). The destruction order makes it observable:
 * unpatched, __destruct runs before run() returns. */
$GLOBALS['log'] = [];

class ProfileHandler
{
    public $ch;
    public function __construct($ch) { $this->ch = $ch; }
    public function run($info)
    {
        $GLOBALS['log'][] = 'run-start';
        $this->ch->setProfileCallback(null);
        $GLOBALS['log'][] = 'run-end';
    }
    public function __destruct() { $GLOBALS['log'][] = 'destruct'; }
}

$ch = new ClickHouse(clickhouse_test_config());
$ch->setProfileCallback([new ProfileHandler($ch), 'run']);

$rows = $ch->select("SELECT number FROM system.numbers LIMIT 100");
echo "rows: ", count($rows), "\n";

$destruct = array_search('destruct', $GLOBALS['log'], true);
$runEnd   = array_search('run-end', $GLOBALS['log'], true);
echo "handler outlived its method: ",
    (($destruct === false || $destruct > $runEnd) ? "yes" : "no"), "\n";

/* The callback unregistered itself, so a second query runs cleanly. */
echo "rows2: ", count($ch->select("SELECT number FROM system.numbers LIMIT 10")), "\n";
?>
--EXPECT--
rows: 100
handler outlived its method: yes
rows2: 10
