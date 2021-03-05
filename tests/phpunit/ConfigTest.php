<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Synapse\Tests;

use Keboola\DbWriter\Configuration\Validator;
use Keboola\DbWriter\Synapse\Configuration\ConfigRowDefinition;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class ConfigTest extends TestCase
{
    /**
     * @dataProvider validConfigDataProvider
     */
    public function testValid(array $configData, array $expectedConfig): void
    {
        $validator = Validator::getValidator(new ConfigRowDefinition());
        $parameters = $validator($configData);

        Assert::assertEquals($expectedConfig, $parameters);
    }

    public function validConfigDataProvider(): iterable
    {
        yield 'minimal-config' => [
            [
                'data_dir' => 'data/dir',
                'tableId' => 'test-table-id',
                'dbName' => 'db-table-name',
            ],
            [
                'data_dir' => 'data/dir',
                'tableId' => 'test-table-id',
                'dbName' => 'db-table-name',
                'incremental' => false,
                'export' => true,
                'primaryKey' => [],
                'items' => [],
            ],
        ];

        yield 'full-config' => [
            [
                'data_dir' => 'data/dir',
                'db' => [
                    'host' => 'test-host',
                    'port' => 1234,
                    'user' => 'test-user',
                    '#password' => 'test-pass',
                    'database' => 'test-db',
                ],
                'tableId' => 'test-table-id',
                'dbName' => 'db-table-name',
                'incremental' => true,
                'export' => false,
                'primaryKey' => [
                    'id',
                ],
                'items' => [
                    [
                        'name' => 'id',
                        'dbName' => 'id',
                        'type' => 'int',
                        'size' => null,
                        'nullable' => false,
                        'default' => null,
                    ],
                    [
                        'name' => 'name',
                        'dbName' => 'name',
                        'type' => 'varchar',
                        'size' => 255,
                        'nullable' => false,
                        'default' => null,
                    ],
                ],
            ],
            [
                'data_dir' => 'data/dir',
                'tableId' => 'test-table-id',
                'dbName' => 'db-table-name',
                'incremental' => true,
                'export' => false,
                'primaryKey' => [
                    'id',
                ],
                'items' => [
                    [
                        'name' => 'id',
                        'dbName' => 'id',
                        'type' => 'int',
                        'size' => null,
                        'nullable' => false,
                        'default' => null,
                    ],
                    [
                        'name' => 'name',
                        'dbName' => 'name',
                        'type' => 'varchar',
                        'size' => 255,
                        'nullable' => false,
                        'default' => null,
                    ],
                ],
                'db' => [
                    'host' => 'test-host',
                    'port' => 1234,
                    'user' => 'test-user',
                    '#password' => 'test-pass',
                    'database' => 'test-db',
                    'schema' => 'dbo',
                    'password' => 'test-pass',
                ],
            ],
        ];
    }
}
