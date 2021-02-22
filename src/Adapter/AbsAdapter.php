<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Synapse\Adapter;

use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Synapse\SynapseWriter;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException;
use MicrosoftAzure\Storage\Common\Internal\Resources;

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

    public function generateCreateStageCommand(string $escapedTableName): string
    {
        $entries = array_map(function ($entry) {
            return SynapseWriter::quote($entry);
        }, $this->getEntries());
        $entries = implode(', ', $entries);
        $credentials = sprintf(
            'IDENTITY=\'Shared Access Signature\', SECRET=\'?%s\'',
            $this->connectionAccessSignature
        );
        $enclosure = SynapseWriter::quote('"');
        $fieldDelimiter = SynapseWriter::quote(',');
        return
            "COPY INTO $escapedTableName FROM $entries WITH (" .
            "FILE_TYPE='CSV', CREDENTIAL=($credentials), FIELDQUOTE=$enclosure, ".
            "FIELDTERMINATOR=$fieldDelimiter, ENCODING = 'UTF8', ROWTERMINATOR='0x0A', IDENTITY_INSERT = 'OFF'".
            ')';
    }

    public function generateCopyCommand(string $escapedTableName, array $columns): string
    {
        throw new \Exception('Rewrite SNFK SQL -> Synapse');
//        $columnNames = array_map(function ($column) {
//            return SynapseWriter::quoteIdentifier($column['dbName']);
//        }, $columns);
//
//        $transformationColumns = array_map(
//            function ($column, $index) {
//                if (!empty($column['nullable'])) {
//                    return sprintf("IFF(t.$%d = '', null, t.$%d)", $index + 1, $index + 1);
//                }
//                return sprintf('t.$%d', $index + 1);
//            },
//            $columns,
//            array_keys($columns)
//        );
//
//        $path = $this->name;
//        $pattern = '';
//        if ($this->isSliced) {
//            // key ends with manifest
//            if (strrpos($this->name, 'manifest') === strlen($this->name) - strlen('manifest')) {
//                $path = substr($this->name, 0, strlen($this->name) - strlen('manifest'));
//                $pattern = 'PATTERN="^.*(?<!manifest)$"';
//            }
//        }
//
//        return sprintf(
//            'COPY INTO %s(%s)
//            FROM (SELECT %s FROM %s t)
//            %s',
//            $tableName,
//            implode(', ', $columnNames),
//            implode(', ', $transformationColumns),
//            SynapseWriter::quote('@' . SynapseWriter::quoteIdentifier($stageName) . '/' . $path),
//            $pattern
//        );
    }

    private function getEntries(): array
    {
        $sasConnectionString = sprintf(
            '%s=https://%s;%s=%s',
            Resources::BLOB_ENDPOINT_NAME,
            $this->connectionEndpoint,
            Resources::SAS_TOKEN_NAME,
            $this->connectionAccessSignature
        );

        $blobClient = BlobRestProxy::createBlobService($sasConnectionString);
        if (!$this->isSliced) {
            // this is temporary solution copy into is not failing when blob not exists
            try {
                $blobClient->getBlob($this->container, $this->name);
            } catch (ServiceException $e) {
                throw new UserException('Load error: ' . $e->getErrorText(), 0, $e);
            }

            [$this->getContainerUrl() . $this->name];
        }

        try {
            $manifestBlob = $blobClient->getBlob($this->container, $this->name);
        } catch (ServiceException $e) {
            throw new UserException('Load error: manifest file was not found.', 0, $e);
        }

        $manifest = \GuzzleHttp\json_decode((string) stream_get_contents($manifestBlob->getContentStream()), true);
        return array_map(function (array $entry) use ($blobClient) {
            // this is temporary solution copy into is not failing when blob not exists
            try {
                /** @var string[] $parts */
                $parts = explode(sprintf('blob.core.windows.net/%s/', $this->container), $entry['url']);
                $blobPath = $parts[1];
                $blobClient->getBlob($this->container, $blobPath);
            } catch (ServiceException $e) {
                throw new UserException('Load error: ' . $e->getErrorText(), 0, $e);
            }
            return str_replace('azure://', 'https://', $entry['url']);
        }, $manifest['entries']);
    }

    private function getContainerUrl(): string
    {
        return sprintf('https://%s/%s', $this->connectionEndpoint, $this->container);
    }
}
