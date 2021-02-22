<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Synapse\Adapter;

use Keboola\DbWriter\Exception\ApplicationException;

class NullAdapter implements IAdapter
{
    public function generateCreateStageCommand(string $escapedTableName): string
    {
        throw new ApplicationException('Method "generateCreateStageCommand" not implemented');
    }

    public function generateCopyCommand(string $escapedTableName, array $columns): string
    {
        throw new ApplicationException('Method "generateCopyCommand" not implemented');
    }
}
