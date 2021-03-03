<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Synapse\FunctionalTests;

use Keboola\DatadirTests\DatadirTestSpecificationInterface;
use PDO;
use RuntimeException;
use Keboola\DbWriter\Synapse\Tests\Traits\DumpTablesTrait;
use Keboola\DbWriter\Synapse\Tests\Traits\GetAllTablesTrait;
use Keboola\DbWriter\Synapse\Tests\Traits\RemoveAllTablesTrait;
use Keboola\DatadirTests\DatadirTestCase;
use Keboola\DbWriter\Synapse\Test\StagingStorageLoader;
use Keboola\DbWriter\Synapse\Tests\Traits\TestConnectionTrait;
use Keboola\StorageApi\Client;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Exception\DirectoryNotFoundException;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Process\Process;

class DatadirTest extends DatadirTestCase
{
    use TestConnectionTrait;
    use GetAllTablesTrait;
    use DumpTablesTrait;
    use RemoveAllTablesTrait;

    protected array $config;

    protected string $testProjectDir;

    protected string $testTempDir;

    protected PDO $connection;

    public function getConnection(): PDO
    {
        return $this->connection;
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Test dir, eg. "/code/tests/functional/full-load-ok"
        $this->testProjectDir = $this->getTestFileDir() . '/' . $this->dataName();
        $this->testTempDir = $this->temp->getTmpFolder();

        // Create test connection
        $this->connection = $this->createConnection();

        // Remove all tables
        $this->removeAllTables();

        // Load setUp.php file - used to init database state
        $setUpPhpFile = $this->testProjectDir . '/setUp.php';
        if (file_exists($setUpPhpFile)) {
            // Get callback from file and check it
            $initCallback = require $setUpPhpFile;
            if (!is_callable($initCallback)) {
                throw new RuntimeException(sprintf('File "%s" must return callback!', $setUpPhpFile));
            }

            // Invoke callback
            $initCallback($this);
        }
    }

    protected function runScript(string $datadirPath): Process
    {
        $this->uploadFixtures($datadirPath);
        return parent::runScript($datadirPath);
    }

    protected function assertMatchesSpecification(
        DatadirTestSpecificationInterface $specification,
        Process $runProcess,
        string $tempDatadir
    ): void {
        $this->dumpAllTables($tempDatadir);
        parent::assertMatchesSpecification($specification, $runProcess, $tempDatadir);
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

    protected function dumpAllTables(string $tmpDir): void
    {
        // Create output dir
        $dumpDir = $tmpDir . '/out/db-dump';
        $fs = new Filesystem();
        $fs->mkdir($dumpDir, 0777);

        // Dump tables
        foreach ($this->getAllTables() as $table) {
            $this->dumpTable($table['TABLE_CATALOG'], $table['TABLE_SCHEMA'], $table['TABLE_NAME'], $dumpDir);
        }
    }
}
