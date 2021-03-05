<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Synapse\Configuration;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;

class DbNode extends ArrayNodeDefinition
{
    public const NODE_NAME = 'db';

    public function __construct()
    {
        parent::__construct(self::NODE_NAME);
        $this->isRequired();
        $this->init($this->children());
    }

    protected function init(NodeBuilder $builder): void
    {
        $builder
            ->scalarNode('host')
                ->isRequired()
            ->end()
            ->integerNode('port')
                ->defaultValue(1433)
            ->end()
            ->scalarNode('user')
                ->isRequired()
            ->end()
            ->scalarNode('#password')
                ->isRequired()
            ->end()
            ->scalarNode('database')
                ->isRequired()
            ->end()
            ->scalarNode('schema')
                ->defaultValue('dbo')
            ->end();
    }
}
