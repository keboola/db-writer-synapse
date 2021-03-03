<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Synapse\Tests\Traits;

use PDO;
use Keboola\DbWriter\Synapse\SynapseWriter;

trait InsertRowsTrait
{
    abstract public function getConnection(): PDO;

    public function insertRows(string $tableName, array $columns, array $rows): void
    {
        // Generate columns statement
        $columnsSql = [];
        foreach ($columns as $name => $sqlDef) {
            $columnsSql[] = SynapseWriter::quoteIdentifier($name);
        }

        // Generate values statement
        $valuesSql = [];
        foreach ($rows as $row) {
            $valuesSql[] =
                '(' .
                implode(
                    ', ',
                    array_map(fn($value) => $value === null ? 'NULL' : SynapseWriter::quote((string) $value), $row)
                ) .
                ')';
        }

        // Insert values
        foreach ($valuesSql as $values) {
            $this->getConnection()->exec(sprintf(
                'INSERT INTO %s (%s) VALUES %s',
                SynapseWriter::quoteIdentifier($tableName),
                implode(', ', $columnsSql),
                $values
            ));
        }
    }
}
