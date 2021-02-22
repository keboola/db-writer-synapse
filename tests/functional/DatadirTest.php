<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Synapse\FunctionalTests;

use Keboola\DatadirTests\DatadirTestCase;
use Keboola\DbWriter\Synapse\Test\StagingStorageLoader;
use Keboola\StorageApi\Client;
use Symfony\Component\Finder\Exception\DirectoryNotFoundException;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Process\Process;

class DatadirTest extends DatadirTestCase
{
    protected array $config;

    protected function runScript(string $datadirPath): Process
    {
        $this->uploadFixtures($datadirPath);
        return parent::runScript($datadirPath);
    }

    protected function uploadFixtures(string $datadirPath): void
    {
        $stagingStorageLoader = new StagingStorageLoader(
            $datadirPath,
            new Client([
                'url' => getenv('KBC_URL'),
                'token' => getenv('STORAGE_API_TOKEN'),
            ])
        );

        $finder = new Finder();
        try {
            $tables = $finder->files()->in($datadirPath . '/in/tables')->name('*.csv');
            foreach ($tables as $table) {
                // Upload file to ABS
                $uploadFileInfo = $stagingStorageLoader->upload(
                    $table->getFilenameWithoutExtension(),
                    (string) $this->dataName()
                );

                // Generate new manifest
                $manifestPath = $table->getPathname() . '.manifest';
                $manifestData = json_decode((string) file_get_contents($manifestPath), true);
                $manifestData[$uploadFileInfo['stagingStorage']] = $uploadFileInfo['manifest'];

                // Remove local file and manifest
                unlink($table->getPathname());
                unlink($manifestPath);

                // Write new manifest
                file_put_contents(
                    $manifestPath,
                    json_encode($manifestData)
                );
            }
        } catch (DirectoryNotFoundException $e) {
            // directory not found -> skip this step
        }
    }

    protected function cleanDb(string $name): void
    {
    }
}
