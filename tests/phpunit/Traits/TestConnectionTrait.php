<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Synapse\Tests\Traits;

use PDO;

trait TestConnectionTrait
{
    public function createConnection(): PDO
    {
        $server = (string) getenv('SYNAPSE_SERVER');
        ;
        $port = (string) getenv('SYNAPSE_PORT');
        $host = $server . ',' . $port;
        $options['Server'] = 'tcp:' . $host;
        $options['Database'] = (string) getenv('SYNAPSE_DATABASE');
        $dsn = sprintf('sqlsrv:%s', implode(';', array_map(function ($key, $item) {
            return sprintf('%s=%s', $key, $item);
        }, array_keys($options), $options)));
        $user = (string) getenv('SYNAPSE_UID');
        $password = (string) getenv('SYNAPSE_PWD');
        ;

        $pdo = new PDO($dsn, $user, $password, [
            'LoginTimeout' => 30,
            'ConnectRetryCount' => 3,
            'ConnectRetryInterval' => 10,
            PDO::SQLSRV_ATTR_QUERY_TIMEOUT => 10800,
        ]);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        return $pdo;
    }
}
