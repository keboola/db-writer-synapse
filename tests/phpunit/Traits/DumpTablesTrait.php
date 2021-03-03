<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Synapse\Tests\Traits;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Synapse\SynapseWriter;
use PDO;

trait DumpTablesTrait
{
    abstract public function getConnection(): PDO;

    public function dumpTable(string $catalog, string $schema, string $name, string $dumpDir): void
    {
        $metadata = $this->getTableMetadata($catalog, $schema, $name);
        $columns = $this->getColumns($catalog, $schema, $name);
        $metadata['columns'] = $columns;

        // Save create statement
        $metadataJson = json_encode($metadata, JSON_PRETTY_PRINT);
        file_put_contents(sprintf('%s/%s.%s.metadata.json', $dumpDir, $schema, $name), $metadataJson);

        // Dump data
        $this->dumpTableData($catalog, $schema, $name, $columns, $dumpDir);
    }

    private function dumpTableData(
        string $catalog,
        string $schema,
        string $name,
        array $columns,
        string $dumpDir
    ): void {
        $csv = new CsvFile(sprintf('%s/%s.%s.data.csv', $dumpDir, $schema, $name));

        // Write header
        $csv->writeRow(array_map(
            fn($col) => $col['name'],
            $columns
        ));

        // Write data, order by first column
        /** @var \PDOStatement $stmt */
        $stmt = $this->getConnection()->query(sprintf(
            'SELECT * FROM %s.%s.%s ORDER BY %s',
            SynapseWriter::quoteIdentifier($catalog),
            SynapseWriter::quoteIdentifier($schema),
            SynapseWriter::quoteIdentifier($name),
            SynapseWriter::quoteIdentifier($columns[0]['name'])
        ));
        /** @var array $rows */
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        foreach ($rows as $row) {
            $csv->writeRow($row);
        }
    }

    private function getTableMetadata(string $catalog, string $schema, string $name): array
    {
        /** @var \PDOStatement $stmt */
        $stmt = $this->getConnection()->query(sprintf(
            'SELECT * FROM "INFORMATION_SCHEMA"."TABLES" ' .
            'WHERE TABLE_CATALOG = %s AND TABLE_SCHEMA = %s AND TABLE_NAME = %s',
            SynapseWriter::quote($catalog),
            SynapseWriter::quote($schema),
            SynapseWriter::quote($name),
        ));

        /** @var array $result */
        $result = $stmt->fetch(PDO::FETCH_ASSOC);
        return [
            'name' => $result['TABLE_NAME'],
            'schema' => $result['TABLE_SCHEMA'],
        ];
    }

    private function getColumns(string $catalog, string $schema, string $name): array
    {
        /** @var \PDOStatement $stmt */
        $stmt = $this->getConnection()->query(sprintf(
            'SELECT * FROM "INFORMATION_SCHEMA"."COLUMNS" ' .
            'WHERE TABLE_CATALOG = %s AND TABLE_SCHEMA = %s AND TABLE_NAME = %s',
            SynapseWriter::quote($catalog),
            SynapseWriter::quote($schema),
            SynapseWriter::quote($name),
        ));

        /** @var array $result */
        $result = $stmt->fetchAll();

        return array_map(function (array $column) {
            return [
                'name' => $column['COLUMN_NAME'],
                'position' => $column['ORDINAL_POSITION'],
                'type' => $column['DATA_TYPE'],
                'is_nullable' => strtoupper($column['IS_NULLABLE']) !== 'NO',
                'default' => $column['COLUMN_DEFAULT'],
                'char_max_length' => $column['CHARACTER_MAXIMUM_LENGTH'],
                'char_octet_length' => $column['CHARACTER_OCTET_LENGTH'],
                'numeric_precision' => $column['NUMERIC_PRECISION'],
                'numeric_precision_radix' => $column['NUMERIC_PRECISION_RADIX'],
                'numeric_scale' => $column['NUMERIC_SCALE'],
            ];
        }, $result);
    }
}
