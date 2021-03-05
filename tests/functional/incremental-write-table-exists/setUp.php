<?php

declare(strict_types=1);

use Keboola\DbWriter\Synapse\FunctionalTests\DatadirTest;
use Keboola\DbWriter\Synapse\FunctionalTests\DatabaseSetupManager;

return function (DatadirTest $test): void {
    $manager = new DatabaseSetupManager($test->getConnection());
    $manager->createIncrementalTable();
    $manager->generateIncrementalTableRows();
};
