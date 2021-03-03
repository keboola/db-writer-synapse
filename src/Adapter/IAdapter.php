<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Synapse\Adapter;

interface IAdapter
{
    public function generateImportToStageSql(string $escapedTableName): string;
}
