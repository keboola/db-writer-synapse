<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Synapse\Adapter;

interface IAdapter
{
    public function generateCreateStageCommand(string $escapedTableName): string;

    public function generateCopyCommand(string $escapedTableName, array $columns): string;
}
