<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Synapse\Configuration;

use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;

class ActionConfigRowDefinition implements ConfigurationInterface
{
    public function getConfigTreeBuilder(): TreeBuilder
    {
        $treeBuilder = new TreeBuilder('parameters');

        $rootNode = $treeBuilder->getRootNode();
        $rootNode
            ->ignoreExtraKeys(false)
            ->children()
                ->append(new DbNode())
                ->scalarNode('data_dir')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
            ->end();

        return $treeBuilder;
    }
}
