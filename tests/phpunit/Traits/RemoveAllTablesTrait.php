<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Synapse\Tests\Traits;

use PDO;
use Keboola\DbWriter\Synapse\SynapseWriter;

trait RemoveAllTablesTrait
{
    use GetAllTablesTrait;

    abstract public function getConnection(): PDO;

    public function removeAllTables(): void
    {
        foreach ($this->getAllTables() as $table) {
            $this->connection->exec(sprintf(
                'DROP TABLE %s.%s.%s',
                SynapseWriter::quoteIdentifier($table['TABLE_CATALOG']),
                SynapseWriter::quoteIdentifier($table['TABLE_SCHEMA']),
                SynapseWriter::quoteIdentifier($table['TABLE_NAME'])
            ));
        }
    }
}
