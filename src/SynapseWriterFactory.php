<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Synapse;

use InvalidArgumentException;
use Keboola\DbWriter\Synapse\Adapter\IAdapter;
use Keboola\DbWriter\Synapse\Adapter\NullAdapter;
use Keboola\DbWriter\WriterFactory;
use Psr\Log\LoggerInterface;

class SynapseWriterFactory extends WriterFactory
{
    private array $parameters;

    public function __construct(array $parameters)
    {
        $this->parameters = $parameters;
        parent::__construct($parameters);
    }

    public function create(LoggerInterface $logger, ?IAdapter $adapter = null): SynapseWriter
    {
        if (!$adapter) {
            $adapter = new NullAdapter();
        }

        return new SynapseWriter($this->parameters['db'], $logger, $adapter);
    }
}
