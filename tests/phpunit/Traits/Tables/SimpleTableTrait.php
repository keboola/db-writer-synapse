<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Synapse\Tests\Traits\Tables;

use Keboola\DbWriter\Synapse\Tests\Traits\CreateTableTrait;
use Keboola\DbWriter\Synapse\Tests\Traits\InsertRowsTrait;

trait SimpleTableTrait
{
    use CreateTableTrait;
    use InsertRowsTrait;

    public function createSimpleTable(string $tableName = 'simple'): void
    {
        $this->createTable($tableName, $this->getSimpleTableColumns());
    }

    public function generateSimpleTableRows(string $tableName = 'simple'): void
    {
        $this->insertRows($tableName, $this->getSimpleTableColumns(), $this->getSimpleTableRows());
    }

    public function getSimpleTableColumns(): array
    {
        return [
            'id' => 'INT NOT NULL',
            'name' => 'VARCHAR(255) NOT NULL', // not null
            'date' => 'DATETIME DEFAULT NULL', // nullable
        ];
    }

    protected function getSimpleTableRows(): array
    {
        return [
            [1, 'Jack Dawson', '2020-10-03 01:02:34'],
            [2, 'Xander Thomas', null],
            [3, 'Jay Macdonald', '2020-10-01 10:20:30'],
        ];
    }
}
