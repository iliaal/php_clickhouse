<?php
/* Prints "ok" when a shutdown function runs after a memory_limit fatal error.
 * Some PHP builds end the process there instead (setup-php's 7.4-8.1 Linux
 * builds), and without the Zend allocator the limit is not enforced. */
$exhausted = true;
register_shutdown_function(function () use (&$exhausted) {
    echo $exhausted ? "ok" : "no memory_limit error";
});
ini_set("memory_limit", "16M");
$a = [];
for ($i = 0; $i < 65536; $i++) {
    $a[] = str_repeat("x", 1024);
}
$exhausted = false;
