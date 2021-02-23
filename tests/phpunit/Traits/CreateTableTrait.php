<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Synapse\Tests\Traits;

use Keboola\DbWriter\Synapse\SynapseWriter;
use PDO;

trait CreateTableTrait
{
    abstract public function getConnection(): PDO;

    public function createTable(string $tableName, array $columns): void
    {
        // Generate columns statement
        $columnsSql = [];
        foreach ($columns as $name => $sqlDef) {
            $columnsSql[] = SynapseWriter::quoteIdentifier($name) . ' ' . $sqlDef;
        }

        // Create table
        $this->getConnection()->exec(sprintf(
            'CREATE TABLE %s (%s)',
            SynapseWriter::quoteIdentifier($tableName),
            implode(', ', $columnsSql)
        ));
    }
}
