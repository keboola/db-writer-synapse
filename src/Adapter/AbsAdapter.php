<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Synapse\Adapter;

use Keboola\DbWriter\Synapse\SynapseWriter;

class AbsAdapter implements IAdapter
{
    private bool $isSliced;

    private string $region;

    private string $container;

    private string $name;

    private string $connectionEndpoint;

    private string $connectionAccessSignature;

    private string $expiration;

    public function __construct(array $absInfo)
    {
        preg_match(
            '/BlobEndpoint=https?:\/\/(.+);SharedAccessSignature=(.+)/',
            $absInfo['credentials']['sas_connection_string'],
            $connectionInfo
        );
        $this->isSliced = $absInfo['is_sliced'];
        $this->region = $absInfo['region'];
        $this->container = $absInfo['container'];
        $this->name = $absInfo['name'];
        $this->connectionEndpoint = $connectionInfo[1];
        $this->connectionAccessSignature = $connectionInfo[2];
        $this->expiration = $absInfo['credentials']['expiration'];
    }

    public function generateCreateStageCommand(string $stageName): string
    {
        $csvOptions = [];
        $csvOptions[] = sprintf('FIELD_DELIMITER = %s', SynapseWriter::quote(','));
        $csvOptions[] = sprintf('FIELD_OPTIONALLY_ENCLOSED_BY = %s', SynapseWriter::quote('"'));
        $csvOptions[] = sprintf('ESCAPE_UNENCLOSED_FIELD = %s', SynapseWriter::quote('\\'));

        if (!$this->isSliced) {
            $csvOptions[] = 'SKIP_HEADER = 1';
        }

        return sprintf(
            "CREATE OR REPLACE STAGE %s
             FILE_FORMAT = (TYPE=CSV %s)
             URL = 'azure://%s/%s'
             CREDENTIALS = (AZURE_SAS_TOKEN = %s)
            ",
            SynapseWriter::quoteIdentifier($stageName),
            implode(' ', $csvOptions),
            $this->connectionEndpoint,
            $this->container,
            SynapseWriter::quote($this->connectionAccessSignature)
        );
    }

    public function generateCopyCommand(string $tableName, string $stageName, array $columns): string
    {
        $columnNames = array_map(function ($column) {
            return SynapseWriter::quoteIdentifier($column['dbName']);
        }, $columns);

        $transformationColumns = array_map(
            function ($column, $index) {
                if (!empty($column['nullable'])) {
                    return sprintf("IFF(t.$%d = '', null, t.$%d)", $index + 1, $index + 1);
                }
                return sprintf('t.$%d', $index + 1);
            },
            $columns,
            array_keys($columns)
        );

        $path = $this->name;
        $pattern = '';
        if ($this->isSliced) {
            // key ends with manifest
            if (strrpos($this->name, 'manifest') === strlen($this->name) - strlen('manifest')) {
                $path = substr($this->name, 0, strlen($this->name) - strlen('manifest'));
                $pattern = 'PATTERN="^.*(?<!manifest)$"';
            }
        }

        return sprintf(
            'COPY INTO %s(%s) 
            FROM (SELECT %s FROM %s t)
            %s',
            $tableName,
            implode(', ', $columnNames),
            implode(', ', $transformationColumns),
            SynapseWriter::quote('@' . SynapseWriter::quoteIdentifier($stageName) . '/' . $path),
            $pattern
        );
    }
}
