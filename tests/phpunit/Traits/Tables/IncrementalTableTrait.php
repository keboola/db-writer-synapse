<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Synapse\Tests\Traits\Tables;

use Keboola\DbWriter\Synapse\Tests\Traits\CreateTableTrait;
use Keboola\DbWriter\Synapse\Tests\Traits\InsertRowsTrait;

trait IncrementalTableTrait
{
    use CreateTableTrait;
    use InsertRowsTrait;

    public function createIncrementalTable(string $tableName = 'incremental'): void
    {
        $this->createTable($tableName, $this->getIncrementalTableColumns());
    }

    public function generateIncrementalTableRows(string $tableName = 'incremental'): void
    {
        $this->insertRows($tableName, $this->getIncrementalTableColumns(), $this->getIncrementalTableRows());
    }

    public function getIncrementalTableColumns(): array
    {
        return [
            'id' => 'INT NOT NULL',
            'value' => 'VARCHAR(255) NOT NULL',
        ];
    }

    protected function getIncrementalTableRows(): array
    {
        return [
            [1, 'value1'],
            [2, 'value2'],
            [3, 'value3'],
        ];
    }
}
