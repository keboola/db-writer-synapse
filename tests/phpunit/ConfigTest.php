<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Synapse\Tests;

use Keboola\CommonExceptions\UserExceptionInterface;
use Keboola\DbWriter\Configuration\Validator;
use Keboola\DbWriter\Synapse\Configuration\ConfigRowDefinition;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class ConfigTest extends TestCase
{
    /**
     * @dataProvider validConfigDataProvider
     */
    public function testValidConfig(array $configData, array $expectedConfig): void
    {
        $validator = Validator::getValidator(new ConfigRowDefinition());
        $parameters = $validator($configData);

        Assert::assertEquals($expectedConfig, $parameters);
    }

    /**
     * @dataProvider invalidConfigDataProvider
     */
    public function testInvalidConfig(array $configData, string $errorMessage): void
    {
        $validator = Validator::getValidator(new ConfigRowDefinition());

        $this->expectException(UserExceptionInterface::class);
        $this->expectExceptionMessage($errorMessage);
        $validator($configData);
    }

    public function validConfigDataProvider(): iterable
    {
        yield 'minimal-config' => [
            [
                'data_dir' => 'data/dir',
                'tableId' => 'test-table-id',
                'dbName' => 'db-table-name',
                'db' => [
                    'host' => 'test-host',
                    'port' => 1234,
                    'user' => 'test-user',
                    '#password' => 'test-pass',
                    'database' => 'test-db',
                ],
            ],
            [
                'data_dir' => 'data/dir',
                'tableId' => 'test-table-id',
                'dbName' => 'db-table-name',
                'incremental' => false,
                'export' => true,
                'primaryKey' => [],
                'items' => [],
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

        yield 'minimal-config-string-port' => [
            [
                'data_dir' => 'data/dir',
                'tableId' => 'test-table-id',
                'dbName' => 'db-table-name',
                'db' => [
                    'host' => 'test-host',
                    'port' => "1234",
                    'user' => 'test-user',
                    '#password' => 'test-pass',
                    'database' => 'test-db',
                ],
            ],
            [
                'data_dir' => 'data/dir',
                'tableId' => 'test-table-id',
                'dbName' => 'db-table-name',
                'incremental' => false,
                'export' => true,
                'primaryKey' => [],
                'items' => [],
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

    public function invalidConfigDataProvider(): iterable
    {
        yield 'missing tableId' => [
            [
                'data_dir' => 'data/dir',
                'db' => [
                    'host' => 'test-host',
                    'port' => 1234,
                    'user' => 'test-user',
                    '#password' => 'test-pass',
                    'database' => 'test-db',
                ],
                'dbName' => 'db-table-name',
            ],
            'The child config "tableId" under "parameters" must be configured.',
        ];

        yield 'missing dbName' => [
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
            ],
            'The child config "dbName" under "parameters" must be configured.',
        ];

        yield 'missing part of db node' => [
            [
                'data_dir' => 'data/dir',
                'dbName' => 'db-table-name',
                'tableId' => 'test-table-id',
                'db' => [
                    'host' => 'test-host',
                    'port' => 1234,
                    '#password' => 'test-pass',
                    'database' => 'test-db',
                ],
            ],
            'The child config "user" under "parameters.db" must be configured.',
        ];

        yield 'missing part of items' => [
            [
                'data_dir' => 'data/dir',
                'dbName' => 'db-table-name',
                'tableId' => 'test-table-id',
                'db' => [
                    'host' => 'test-host',
                    'port' => 1234,
                    'user' => 'test-user',
                    '#password' => 'test-pass',
                    'database' => 'test-db',
                ],
                'items' => [
                    [
                        'name' => 'id',
                        'type' => 'int',
                    ],
                ],
            ],
            'The child config "dbName" under "parameters.items.0" must be configured.',
        ];

        yield 'invalid port value' => [
            [
                'data_dir' => 'data/dir',
                'dbName' => 'db-table-name',
                'tableId' => 'test-table-id',
                'db' => [
                    'host' => 'test-host',
                    'port' => "invalidPort",
                    'user' => 'test-user',
                    '#password' => 'test-pass',
                    'database' => 'test-db',
                ],
            ],
            'Port "invalidPort" has not a numeric value.',
        ];
    }
}
