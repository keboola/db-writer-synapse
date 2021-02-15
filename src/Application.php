<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Synapse;

use Exception;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Application as BaseApplication;
use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Synapse\Adapter\AbsAdapter;
use Keboola\DbWriter\Synapse\Adapter\IAdapter;
use Keboola\DbWriter\Synapse\Configuration\ActionConfigRowDefinition;
use Keboola\DbWriter\Synapse\Configuration\ConfigRowDefinition;
use Psr\Log\LoggerInterface;
use SplFileInfo;

class Application extends BaseApplication
{
    public const STORAGE_ABS = 'abs';

    public const STORAGE_S3 = 's3';

    public function __construct(LoggerInterface $logger)
    {
        $dataDir = getenv('KBC_DATADIR') ?: '/data';
        $config = json_decode(file_get_contents($dataDir . '/config.json'), true);
        $config['parameters'] = $config['parameters'] ?? [];
        $config['parameters']['data_dir'] = $dataDir;

        $action = $config['action'] ?? 'run';
        if (isset($config['parameters']['tables'])) {
            throw new ApplicationException('Old config format is not supported. Please, use row configuration.');
        } else {
            if ($action === 'run') {
                $configDefinition = new ConfigRowDefinition();
            } else {
                $configDefinition = new ActionConfigRowDefinition();
            }
        }


        parent::__construct($config, $logger, $configDefinition);

        $app = $this;
        $this['writer_factory'] = function () use ($app) {
            return $this->getWriterFactory($app['parameters']);
        };
    }

    public function runAction(): string
    {
        if (isset($this['parameters']['tables'])) {
            $tables = array_filter((array) $this['parameters']['tables'], function ($table) {
                return ($table['export']);
            });
            foreach ($tables as $key => $tableConfig) {
                $tables[$key] = $this->processRunAction($tableConfig);
            }
        } elseif (!isset($this['parameters']['export']) || $this['parameters']['export']) {
            $this->processRunAction($this['parameters']);
        }
        return 'Writer finished successfully';
    }

    private function processRunAction(array $tableConfig): array
    {
        $manifest = $this->getManifest($tableConfig['tableId']);
        $tableConfig['items'] = $this->reorderColumnsFromArray($manifest['columns'], $tableConfig['items']);

        if (empty($tableConfig['items'])) {
            return $tableConfig;
        }

        try {
            $adapter = $this->getAdapter($manifest);
            if (isset($tableConfig['incremental']) && $tableConfig['incremental']) {
                $this->writeIncrementalFromAdapter($tableConfig, $adapter);
            } else {
                $this->writeFullFromAdapter($tableConfig, $adapter);
            }
        } catch (Exception $e) {
            $this['logger']->error($e->getMessage());
            throw new UserException($e->getMessage(), 0, $e);
        } catch (UserException $e) {
            $this['logger']->error($e->getMessage());
            throw $e;
        } catch (\Throwable $e) {
            throw new ApplicationException($e->getMessage(), 2, $e);
        }

        return $tableConfig;
    }

    public function writeFull(SplFileInfo $csv, array $tableConfig): void
    {
        throw new ApplicationException('Method not implemented');
    }

    public function writeIncremental(SplFileInfo $csv, array $tableConfig): void
    {
        throw new ApplicationException('Method not implemented');
    }

    public function writeIncrementalFromAdapter(array $tableConfig, IAdapter $adapter): void
    {
        /** @var SynapseWriter $writer */
        $writer = $this['writer_factory']->create($this['logger'], $adapter);

        // write to staging table
        $stageTable = $tableConfig;
        $stageTable['dbName'] = $writer->generateTmpName($tableConfig['dbName']);

        $writer->drop($stageTable['dbName']);
        $writer->createStaging($stageTable);
        $writer->writeFromAdapter($stageTable);

        // create destination table if not exists
        $dstTableExists = $writer->tableExists($tableConfig['dbName']);
        if (!$dstTableExists) {
            $writer->create($tableConfig);
        }
        $writer->validateTable($tableConfig);

        // upsert from staging to destination table
        $writer->upsert($stageTable, $tableConfig['dbName']);
    }

    public function writeFullFromAdapter(array $tableConfig, IAdapter $adapter): void
    {
        /** @var SynapseWriter $writer */
        $writer = $this['writer_factory']->create($this['logger'], $adapter);

        $stagingTableName = uniqid('staging');
        $stagingTableConfig = array_merge($tableConfig, [
            'dbName' => $stagingTableName,
        ]);
        $writer->create($stagingTableConfig);
        try {
            // create dummy table for first load which will be replaced by tables swap
            $writer->createIfNotExists($tableConfig);
            $writer->writeFromAdapter($stagingTableConfig);
            $writer->swapTables($tableConfig['dbName'], $stagingTableName);
        } finally {
            $writer->drop($stagingTableName);
        }
    }

    private function getManifest(string $tableId): array
    {
        return json_decode(
            (string) file_get_contents($this['parameters']['data_dir'] . '/in/tables/' . $tableId . '.csv.manifest'),
            true
        );
    }

    private function getAdapter(array $manifest): IAdapter
    {
        if (isset($manifest[self::STORAGE_S3])) {
            throw new ApplicationException('S3 staging storage is not implemented.');
        }
        if (isset($manifest[self::STORAGE_ABS])) {
            return new AbsAdapter($manifest[self::STORAGE_ABS]);
        }
        throw new UserException('Unknown staging storage');
    }

    protected function getWriterFactory(array $parameters): SynapseWriterFactory
    {
        return new SynapseWriterFactory($parameters);
    }

    protected function reorderColumnsFromArray(array $csvHeader, array $items): array
    {
        $reordered = [];
        foreach ($csvHeader as $csvCol) {
            foreach ($items as $item) {
                if ($csvCol === $item['name']) {
                    $reordered[] = $item;
                }
            }
        }

        return $reordered;
    }
}
