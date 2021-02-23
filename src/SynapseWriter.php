<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Synapse;

use PDO;
use SplFileInfo;
use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Synapse\Adapter\IAdapter;
use Keboola\DbWriter\Writer;
use Keboola\DbWriter\WriterInterface;
use Psr\Log\LoggerInterface;

class SynapseWriter extends Writer implements WriterInterface
{
    public const STAGE_NAME = 'db-writer';

    /** @var string[]  */
    private static array $allowedTypes = [
        'decimal', 'numeric', 'float', 'real', 'money', 'smallmoney', 'bigint', 'int', 'smallint',
        'tinyint', 'bit', 'nvarchar', 'nchar', 'varchar', 'char', 'varbinary', 'binary', 'uniqueidentifier',
        'datetimeoffset', 'datetime2', 'datetime', 'smalldatetime', 'date', 'time',
    ];

    /** @var string[]  */
    private static array $typesWithSize = [
        'decimal', 'numeric', 'float', 'nvarchar', 'nchar', 'varchar', 'char', 'varbinary', 'binary', 'time',
    ];

    protected IAdapter $adapter;

    public static function getAllowedTypes(): array
    {
        return self::$allowedTypes;
    }

    public function __construct(array $dbParams, LoggerInterface $logger, IAdapter $adapter)
    {
        parent::__construct($dbParams, $logger);
        $this->adapter = $adapter;
    }

    public function showTables(string $dbName): array
    {
        throw new ApplicationException('Method not implemented');
    }

    public function getTableInfo(string $tableName): array
    {
        throw new ApplicationException('Method not implemented');
    }

    public function write(SplFileInfo $csv, array $table): void
    {
        throw new ApplicationException('Method not implemented');
    }

    public function createConnection(array $dbParams): PDO
    {
        $host = $dbParams['host'] . ',' . $dbParams['port'];
        $options['Server'] = 'tcp:' . $host;
        $options['Database'] = $dbParams['database'];
        $dsn = sprintf('sqlsrv:%s', implode(';', array_map(function ($key, $item) {
            return sprintf('%s=%s', $key, $item);
        }, array_keys($options), $options)));
        $this->logger->info("Connecting to DSN '" . $dsn . "'");

        $pdo = new PDO($dsn, $dbParams['user'], $dbParams['password'], [
            'LoginTimeout' => 30,
            'ConnectRetryCount' => 3,
            'ConnectRetryInterval' => 10,
            PDO::SQLSRV_ATTR_QUERY_TIMEOUT => 10800,
        ]);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        return $pdo;
    }

    public function createStaging(array $table): void
    {
        $sqlDefinitions = [$this->getColumnsSqlDefinition($table)];
        if (!empty($table['primaryKey'])) {
            //$sqlDefinitions [] = $this->getPrimaryKeySqlDefinition($table['primaryKey']);
        }

        $this->execQuery(sprintf(
            'CREATE TABLE %s (%s);',
            $this->nameWithSchemaEscaped($table['dbName']),
            implode(', ', $sqlDefinitions)
        ));
    }

    public function writeFromAdapter(array $stageTable): void
    {
        $escapedTableName = $this->nameWithSchemaEscaped($stageTable['dbName']);
        $this->execQuery($this->adapter->generateImportToStageSql($escapedTableName));
    }

    protected function nameWithSchemaEscaped(string $tableName, ?string $schemaName = null): string
    {
        if ($schemaName === null) {
            $schemaName = $this->dbParams['schema'];
        }
        return sprintf(
            '%s.%s',
            self::quoteIdentifier($schemaName),
            self::quoteIdentifier($tableName)
        );
    }

    public static function quote(string $value): string
    {
        $q = "'";
        return ($q . str_replace("$q", "$q$q", $value) . $q);
    }

    public static function quoteIdentifier(string $value): string
    {
        $q = '"';
        return ($q . str_replace("$q", "$q$q", $value) . $q);
    }

    public function drop(string $tableName): void
    {
        $this->execQuery(sprintf(
            'IF OBJECT_ID(N\'%s\', N\'U\') IS NOT NULL DROP TABLE %s;',
            $this->nameWithSchemaEscaped($tableName),
            $this->nameWithSchemaEscaped($tableName)
        ));
    }

    public function create(array $table): void
    {
        $sqlDefinitions = [$this->getColumnsSqlDefinition($table)];
        if (!empty($table['primaryKey'])) {
            //$sqlDefinitions [] = $this->getPrimaryKeySqlDefinition($table['primaryKey']);
        }

        $this->execQuery(sprintf(
            'CREATE TABLE %s (%s);',
            self::quoteIdentifier($table['dbName']),
            implode(', ', $sqlDefinitions)
        ));
    }

    public function createIfNotExists(array $table): void
    {
        $sqlDefinitions = [$this->getColumnsSqlDefinition($table)];
        if (!empty($table['primaryKey'])) {
            //$sqlDefinitions [] = $this->getPrimaryKeySqlDefinition($table['primaryKey']);
        }

        $this->execQuery(sprintf(
            'CREATE TABLE IF NOT EXISTS %s (%s);',
            self::quoteIdentifier($table['dbName']),
            implode(', ', $sqlDefinitions)
        ));
    }

    public function swapTables(string $table1, string $table2): void
    {
        $this->execQuery(sprintf(
            'ALTER TABLE %s SWAP WITH %s',
            self::quoteIdentifier($table2),
            self::quoteIdentifier($table1)
        ));
    }

    public function upsert(array $table, string $targetTable): void
    {
        if (!empty($table['primaryKey'])) {
            $this->addPrimaryKeyIfMissing($table['primaryKey'], $targetTable);

            // check primary keys
            $this->checkPrimaryKey($table['primaryKey'], $targetTable);
        }

        $sourceTable = $this->nameWithSchemaEscaped($table['dbName']);
        $targetTable = $this->nameWithSchemaEscaped($targetTable);

        $columns = array_map(
            function ($item) {
                return self::quoteIdentifier($item['dbName']);
            },
            array_filter($table['items'], function ($item) {
                return strtolower($item['type']) !== 'ignore';
            })
        );

        if (!empty($table['primaryKey'])) {
            // update data
            $joinClauseArr = [];
            foreach ($table['primaryKey'] as $index => $value) {
                $joinClauseArr[] = sprintf(
                    '%s.%s=%s.%s',
                    $targetTable,
                    self::quoteIdentifier($value),
                    $sourceTable,
                    self::quoteIdentifier($value)
                );
            }
            $joinClause = implode(' AND ', $joinClauseArr);

            $valuesClauseArr = [];
            foreach ($columns as $index => $column) {
                $valuesClauseArr[] = sprintf(
                    '%s=%s.%s',
                    $column,
                    $sourceTable,
                    $column
                );
            }
            $valuesClause = implode(',', $valuesClauseArr);

            $this->execQuery(sprintf(
                'UPDATE %s SET %s FROM %s WHERE %s',
                $targetTable,
                $valuesClause,
                $sourceTable,
                $joinClause
            ));

            // delete updated from temp table
            $this->execQuery(sprintf(
                'DELETE FROM %s USING %s WHERE %s',
                $sourceTable,
                $targetTable,
                $joinClause
            ));
        }

        // insert new data
        $columnsClause = implode(',', $columns);
        $query = "INSERT INTO {$targetTable} ({$columnsClause}) SELECT * FROM {$sourceTable}";
        $this->execQuery($query);

        // drop temp table
        $this->drop($table['dbName']);
    }

    public function tableExists(string $tableName): bool
    {
        $res = $this->fetchAll(sprintf(
            '
                SELECT *
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME = %s
                AND TABLE_SCHEMA = %s
                AND TABLE_CATALOG = %s
            ',
            $this->quote($tableName),
            $this->quote($this->dbParams['schema']),
            $this->quote($this->dbParams['database'])
        ));

        return !empty($res);
    }

    private function execQuery(string $query): void
    {
        $this->logger->info(sprintf("Executing query '%s'", $this->hideCredentialsInQuery($query)));
        try {
            $this->db->exec($query);
        } catch (\Throwable $e) {
            throw new UserException('Query execution error: ' . $e->getMessage(), 0, $e);
        }
    }

    private function fetchAll(string $query): array
    {
        /** @var \PDOStatement $stmt */
        $stmt = $this->db->query($query);
        /** @var array $result */
        $result = $stmt->fetchAll(PDO::FETCH_ASSOC);
        return $result;
    }


    public function testConnection(): void
    {
        $this->execQuery('SELECT 1');
    }

    public function generateTmpName(string $tableName): string
    {
        $tmpId = '_' . str_replace('.', '_', uniqid('wr_db_', true));
        return '#' . mb_substr($tableName, 0, 256 - mb_strlen($tmpId)) . $tmpId;
    }

    /**
     * Generate stage name for given run ID
     *
     * @param string $runId
     * @return string
     */
    public function generateStageName(string $runId): string
    {
        return rtrim(
            mb_substr(
                sprintf(
                    '%s-%s',
                    self::STAGE_NAME,
                    str_replace('.', '-', $runId)
                ),
                0,
                255
            ),
            '-'
        );
    }

    public function getCurrentUser(): string
    {
        return $this->fetchAll('SELECT CURRENT_USER;')[0]['CURRENT_USER'];
    }

    public function checkPrimaryKey(array $columns, string $targetTable): void
    {
//        $primaryKeysInDb = $this->db->getTablePrimaryKey($this->dbParams['schema'], $targetTable);
//
//        sort($primaryKeysInDb);
//        sort($columns);
//
//        if ($primaryKeysInDb !== $columns) {
//            throw new UserException(sprintf(
//                'Primary key(s) in configuration does NOT match with keys in DB table.' . PHP_EOL
//                . 'Keys in configuration: %s' . PHP_EOL
//                . 'Keys in DB table: %s',
//                implode(',', $columns),
//                implode(',', $primaryKeysInDb)
//            ));
//        }
    }

    private function addPrimaryKeyIfMissing(array $columns, string $targetTable): void
    {
//        $primaryKeysInDb = $this->db->getTablePrimaryKey($this->dbParams['schema'], $targetTable);
//        if (!empty($primaryKeysInDb)) {
//            return;
//        }
//
//        $sql = sprintf(
//            'ALTER TABLE %s ADD %s;',
//            $this->nameWithSchemaEscaped($targetTable),
//            $this->getPrimaryKeySqlDefinition($columns)
//        );
//
//        $this->execQuery($sql);
    }

    private function getColumnsSqlDefinition(array $table): string
    {
        $columns = array_filter((array) $table['items'], function ($item) {
            return (strtolower($item['type']) !== 'ignore');
        });

        $sql = '';

        foreach ($columns as $col) {
            $type = strtoupper($col['type']);
            if (!empty($col['size']) && in_array(strtolower($col['type']), self::$typesWithSize)) {
                $type .= sprintf('(%s)', $col['size']);
            }
            $null = $col['nullable'] ? 'NULL' : 'NOT NULL';
            $default = empty($col['default']) ? '' : "DEFAULT '{$col['default']}'";
            if ($type === 'TEXT') {
                $default = '';
            }
            $sql .= sprintf(
                '%s %s %s %s,',
                self::quoteIdentifier($col['dbName']),
                $type,
                $null,
                $default
            );
        }

        return trim($sql, ' ,');
    }

//    private function getPrimaryKeySqlDefinition(array $primaryColumns): string
//    {
//        return '';
////        $writer = $this;
////
////        return sprintf(
////            'PRIMARY KEY(%s)',
////            implode(
////                ', ',
////                array_map(
////                    function ($primaryColumn) use ($writer) {
////                        return $writer->quoteIdentifier($primaryColumn);
////                    },
////                    $primaryColumns
////                )
////            )
////        );
//    }

    private function hideCredentialsInQuery(string $query): ?string
    {
        return preg_replace(
            '/(AZURE_[A-Z_]*\\s=\\s.|AWS_[A-Z_]*\\s=\\s.)[0-9A-Za-z\\/\\+=\\-&:%]*/',
            '${1}...\'',
            $query
        );
    }

    public function validateTable(array $tableConfig): void
    {
    }

    public function getConnection(): \PDO
    {
        throw new ApplicationException('Method not implemented');
    }
}
