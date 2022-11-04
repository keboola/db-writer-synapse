<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Synapse;

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

    private array $parameters;

    public function __construct(LoggerInterface $logger)
    {
        $dataDir = getenv('KBC_DATADIR') ?: '/data';
        $config = json_decode((string) file_get_contents($dataDir . '/config.json'), true);
        $config['parameters'] = $config['parameters'] ?? [];
        $config['parameters']['data_dir'] = $dataDir;
        if (isset($config['image_parameters']['global_config']['absCredentialsType']) &&
            empty($config['parameters']['absCredentialsType'])) {
            $config['parameters']['absCredentialsType'] =
                $config['image_parameters']['global_config']['absCredentialsType'];
        }

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
        $this['authorization'] = $config['authorization'] ?? [];
        $this->parameters = $this['parameters'];

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

            /** @var SynapseWriter $writer */
            $writer = $this['writer_factory']->create($this['logger'], $adapter);

            $defaultWlmContext = sprintf('%s-writer', getenv('KBC_PROJECTID'));
            $wlmContext = $this['authorization']['context'] ?? $defaultWlmContext;
            $writer->setWlmContext($wlmContext);

            if (isset($tableConfig['incremental']) && $tableConfig['incremental']) {
                $this->writeIncrementalFromAdapter($writer, $tableConfig);
            } else {
                $this->writeFullFromAdapter($writer, $tableConfig);
            }

            $writer->setWlmContext();
        } catch (UserException $e) {
            throw $e;
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

    public function writeIncrementalFromAdapter(SynapseWriter $writer, array $tableConfig): void
    {
        // create staging table
        $stageTable = $tableConfig;
        $stageTable['dbName'] = $writer->generateTmpName($tableConfig['dbName']);
        $writer->createStaging($stageTable);
        $writer->writeFromAdapter($stageTable);

        // create destination table if not exists
        $writer->createIfNotExists($tableConfig);
        $writer->validateTable($tableConfig);

        // upsert from staging to destination table
        $writer->upsert($stageTable, $tableConfig['dbName']);
    }

    public function writeFullFromAdapter(SynapseWriter $writer, array $tableConfig): void
    {
        // create staging table
        $stageTable = $tableConfig;
        $stageTable['dbName'] = $writer->generateStageName($tableConfig['dbName']);
        $writer->drop($stageTable['dbName']);
        $writer->create($stageTable);

        try {
            // create target table
            $writer->createIfNotExists($tableConfig);
            $writer->writeFromAdapter($stageTable);
            $writer->swapTables($tableConfig['dbName'], $stageTable['dbName']);
        } finally {
            $writer->drop($stageTable['dbName']);
        }
    }

    private function getManifest(string $tableId): array
    {
        $tableManifestPath = $this['parameters']['data_dir'] . '/in/tables/' . $tableId . '.csv.manifest';
        if (!file_exists($tableManifestPath)) {
            throw new UserException(sprintf(
                'Table "%s" in storage input mapping cannot be found.',
                $tableId
            ));
        }
        return json_decode(
            (string) file_get_contents($tableManifestPath),
            true
        );
    }

    private function getAdapter(array $manifest): IAdapter
    {
        if (isset($manifest[self::STORAGE_ABS])) {
            return new AbsAdapter($manifest[self::STORAGE_ABS], $this->parameters['absCredentialsType']);
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
