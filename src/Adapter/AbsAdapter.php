<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Synapse\Adapter;

use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Synapse\SynapseWriter;
use Keboola\FileStorage\Abs\RetryMiddlewareFactory;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException;
use MicrosoftAzure\Storage\Common\Internal\Resources;

class AbsAdapter implements IAdapter
{
    public const CREDENTIALS_TYPE_SAS = 'sas';
    public const CREDENTIALS_TYPE_MANAGED_IDENTITY = 'managed_identity';

    private bool $isSliced;

    private string $region;

    private string $container;

    private string $name;

    private string $connectionEndpoint;

    private string $connectionAccessSignature;

    private string $expiration;

    private string $credentialsType;

    public function __construct(array $absInfo, string $credentialsType)
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
        $this->credentialsType = $credentialsType;
    }

    public function generateImportToStageSql(string $escapedTableName): string
    {
        $entries = array_map(function ($entry) {
            return SynapseWriter::quote($entry);
        }, $this->getEntries());

        if (empty($entries)) {
            throw new NoEntriesException();
        }

        $entries = implode(', ', $entries);
        $credentials = $this->getCredentials();
        $enclosure = SynapseWriter::quote('"');
        $fieldDelimiter = SynapseWriter::quote(',');

        sleep(5);
        return
            "COPY INTO $escapedTableName FROM $entries WITH (" .
            "FILE_TYPE='CSV', CREDENTIAL=($credentials), FIELDQUOTE=$enclosure, ".
            "FIELDTERMINATOR=$fieldDelimiter, ENCODING = 'UTF8', ROWTERMINATOR='0x0A', IDENTITY_INSERT = 'OFF'".
            ');';
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
        $blobClient->pushMiddleware(RetryMiddlewareFactory::create());
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

    private function getCredentials(): string
    {
        switch ($this->credentialsType) {
            case self::CREDENTIALS_TYPE_SAS:
                return sprintf(
                    'IDENTITY=\'Shared Access Signature\', SECRET=\'?%s\'',
                    $this->connectionAccessSignature
                );

            case self::CREDENTIALS_TYPE_MANAGED_IDENTITY:
                return 'IDENTITY=\'Managed Identity\'';

            default:
                throw new \InvalidArgumentException(sprintf(
                    'Unknown credentials type "%s".',
                    $this->credentialsType
                ));
        }
    }

    private function getContainerUrl(): string
    {
        return sprintf('https://%s/%s', $this->connectionEndpoint, $this->container);
    }
}
