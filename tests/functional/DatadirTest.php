<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Synapse\FunctionalTests;

use _HumbugBoxfac515c46e83\Nette\DirectoryNotFoundException;
use Keboola\DatadirTests\DatadirTestCase;
use Keboola\DbWriter\Synapse\Test\StagingStorageLoader;
use Keboola\StorageApi\Client;
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

    protected function uploadFixtures(string $datadirPath): void {
        $stagingStorageLoader = new StagingStorageLoader(
            $datadirPath,
            new Client([
                'url' =>getenv('KBC_URL'),
                'token' => getenv('STORAGE_API_TOKEN'),
            ])
        );

        $finder = new Finder();
        var_dump('xxx');
        var_dump(scandir($datadirPath . '/in/tables'));
        try {
            $tables = $finder->files()->in($datadirPath . '/in/tables')->name('*.csv');
            foreach ($tables as $table) {
                $uploadFileInfo = $stagingStorageLoader->upload($table->getFilenameWithoutExtension());
                var_dump($uploadFileInfo);
            }
        } catch (DirectoryNotFoundException $e) {
            // directory not found -> skip this step
        }

        exit(1);

//        $dstManifestPath = $tmpRunDir . '/in/tables/' . $table['tableId'] . '.csv.manifest';
//        file_put_contents(
//            $dstManifestPath,
//            json_encode($manifestData)
//        );
    }

    protected function cleanDb(string $name): void
    {

    }
}
