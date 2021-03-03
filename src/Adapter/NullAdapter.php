<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Synapse\Adapter;

use Keboola\DbWriter\Exception\ApplicationException;

class NullAdapter implements IAdapter
{
    public function generateImportToStageSql(string $escapedTableName): string
    {
        throw new ApplicationException('Method "generateCreateStageCommand" not implemented');
    }
}
