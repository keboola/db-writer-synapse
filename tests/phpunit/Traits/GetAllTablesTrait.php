<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Synapse\Tests\Traits;

use PDO;

trait GetAllTablesTrait
{
    abstract public function getConnection(): PDO;

    public function getAllTables(): array
    {
        /** @var \PDOStatement $stmt */
        $stmt = $this->getConnection()->query('SELECT * FROM "INFORMATION_SCHEMA"."TABLES"');
        /** @var array $tables */
        $tables = $stmt->fetchAll();
        return $tables;
    }
}
