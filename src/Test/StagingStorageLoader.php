<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Synapse\Test;

use Keboola\DbWriter\Synapse\Application;
use RuntimeException;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\GetFileOptions;

class StagingStorageLoader
{
    private const CACHE_PATH = __DIR__ . '/../../tests/.cache/StagingStorageLoader.cache.json';

    private array $fileInfoCache;

    private string $dataDir;

    private Client $storageApi;

    public function __construct(string $dataDir, Client $storageApiClient)
    {
        $this->dataDir = $dataDir;
        $this->storageApi = $storageApiClient;
        $this->fileInfoCache = @json_decode((string) @file_get_contents(self::CACHE_PATH), true) ?: [];
    }

    public function __destruct()
    {
        file_put_contents(self::CACHE_PATH, @json_encode($this->fileInfoCache));
    }

    private function getInputCsv(string $tableId): string
    {
        return sprintf($this->dataDir . '/in/tables/%s.csv', $tableId);
    }

    public function upload(string $tableId, string $testName): array
    {
        // Load from cache
        $cacheKey = $testName . '-' . $tableId;
        if (isset($this->fileInfoCache[$cacheKey])) {
            $result = $this->fileInfoCache[$cacheKey];
            try {
                $this->storageApi->getFile($result['fileId']);
                return $result;
            } catch (\Throwable $e) {
                // re-upload if an error
            }
        }

        // Upload
        $filePath = $this->getInputCsv($tableId);
        $bucketId = 'test-wr-db-synapse';
        if (!$this->storageApi->bucketExists('in.c-' . $bucketId)) {
            $this->storageApi->createBucket($bucketId, Client::STAGE_IN);
        }

        $sourceTableId = $this->storageApi->createTable('in.c-' .$bucketId, $tableId, new CsvFile($filePath));
        $this->storageApi->writeTable($sourceTableId, new CsvFile($filePath));
        $job = $this->storageApi->exportTableAsync($sourceTableId, ['gzip' => true]);
        $fileInfo = $this->storageApi->getFile(
            $job['file']['id'],
            (new GetFileOptions())->setFederationToken(true)
        );

        if (!isset($fileInfo['absPath'])) {
            throw new RuntimeException('Only ABS staging storage is supported.');
        }

        $result = [
            'fileId' => $job['file']['id'],
            'stagingStorage' => Application::STORAGE_ABS,
            'manifest' => $this->getAbsManifest($fileInfo),
        ];

        $this->fileInfoCache[$cacheKey] = $result;
        return $result;
    }

    private function getAbsManifest(array $fileInfo): array
    {
        return [
            'is_sliced' => $fileInfo['isSliced'],
            'region' => $fileInfo['region'],
            'container' => $fileInfo['absPath']['container'],
            'name' => $fileInfo['isSliced'] ? $fileInfo['absPath']['name'] . 'manifest' : $fileInfo['absPath']['name'],
            'credentials' => [
                'sas_connection_string' => $fileInfo['absCredentials']['SASConnectionString'],
                'expiration' => $fileInfo['absCredentials']['expiration'],
            ],
        ];
    }
}
