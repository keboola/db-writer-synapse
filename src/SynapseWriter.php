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
    public const STAGE_NAME = '_db_writer_stage';

    public const TEMP_NAME = '_db_writer_temp';

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

    public function create(array $table): void
    {
        $with = [];
        $primaryKey = $table['primaryKey'] ?? [];
        if (count($primaryKey) === 1) {
            $with[] = sprintf(
                'DISTRIBUTION = HASH(%s)',
                SynapseWriter::quoteIdentifier($table['primaryKey'][0])
            );
        }

        if (count($primaryKey) > 0) {
            $with[] = sprintf(
                'CLUSTERED INDEX (%s)',
                implode(', ', array_map(
                    fn(string $column) => SynapseWriter::quoteIdentifier((string) $column),
                    $primaryKey
                ))
            );
        }

        $withSql = $with ? sprintf(' WITH (%s)', implode(', ', $with)) : '';
        $this->execQuery(sprintf(
            'CREATE TABLE %s (%s)%s;',
            $this->nameWithSchemaEscaped($table['dbName']),
            $this->getColumnsSqlDefinition($table),
            $withSql,
        ));
    }

    public function createIfNotExists(array $table): void
    {
        if ($this->tableExists($table['dbName'])) {
            return;
        }
        $this->create($table);
    }

    public function createStaging(array $table): void
    {
        $this->execQuery(sprintf(
            'CREATE TABLE %s (%s);',
            $this->nameWithSchemaEscaped($table['dbName']),
            $this->getColumnsSqlDefinition($table)
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

    public function swapTables(string $table1, string $table2): void
    {
        $sql = [];
        $sql[] = sprintf(
            'RENAME OBJECT %s TO %s;',
            $this->nameWithSchemaEscaped($table1),
            SynapseWriter::quoteIdentifier($table1 . '_old')
        );
        $sql[] = sprintf(
            'RENAME OBJECT %s TO %s;',
            $this->nameWithSchemaEscaped($table2),
            SynapseWriter::quoteIdentifier($table1)
        );
        $sql[] = sprintf(
            'RENAME OBJECT %s TO %s;',
            $this->nameWithSchemaEscaped($table1 . '_old'),
            SynapseWriter::quoteIdentifier($table2)
        );

        $this->execQuery(implode(' ', $sql));
    }

    public function upsert(array $table, string $targetTable): void
    {
        $tempTable = $this->nameWithSchemaEscaped($table['dbName']);
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
                    'target',
                    self::quoteIdentifier($value),
                    'temp',
                    self::quoteIdentifier($value)
                );
            }
            $joinClause = implode(' AND ', $joinClauseArr);

            $valuesClauseArr = [];
            foreach ($columns as $index => $column) {
                $valuesClauseArr[] = sprintf(
                    '%s=%s.%s',
                    $column,
                    'temp',
                    $column
                );
            }
            $valuesClause = implode(',', $valuesClauseArr);

            // update target table from temp table
            $this->execQuery(sprintf(
                'UPDATE target SET %s FROM %s AS target INNER JOIN %s AS temp ON %s',
                $valuesClause,
                $targetTable,
                $tempTable,
                $joinClause
            ));

            // delete updated from temp table
            $this->execQuery(sprintf(
                'DELETE temp FROM %s AS temp INNER JOIN %s AS target ON %s',
                $tempTable,
                $targetTable,
                $joinClause
            ));
        }

        // insert new data
        $columnsClause = implode(',', $columns);
        $query = "INSERT INTO {$targetTable} ({$columnsClause}) SELECT * FROM {$tempTable}";
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
        // "#" is a mark for the temp table
        return rtrim(
            mb_substr(
                sprintf(
                    '#%s_%s',
                    self::TEMP_NAME,
                    str_replace('.', '_', $tableName)
                ),
                0,
                255
            ),
            '-'
        );
    }

    public function generateStageName(string $tableName): string
    {
        return rtrim(
            mb_substr(
                sprintf(
                    '%s_%s',
                    self::STAGE_NAME,
                    str_replace('.', '_', $tableName)
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

    private function hideCredentialsInQuery(string $query): ?string
    {
        return preg_replace(
            '/(SECRET=)\'[^\']+\'/i',
            '$1\'...\'',
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
