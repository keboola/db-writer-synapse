<?php

namespace Keboola\DbWriter\Synapse\Configuration;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class DbNode extends ArrayNodeDefinition
{
    public const NODE_NAME = 'db';

    public function __construct()
    {
        parent::__construct(self::NODE_NAME);
        $this->init($this->children());
    }

    protected function init(NodeBuilder $builder): void
    {
//        $this->addDriverNode($builder);
//        $this->addHostNode($builder);
//        $this->addPortNode($builder);
//        $this->addDatabaseNode($builder);
//        $this->addUserNode($builder);
//        $this->addPasswordNode($builder);
//        $this->addSshNode($builder);
//        $this->addSslNode($builder);
    }

    public function getConfigTreeBuilder(): TreeBuilder
    {
        $treeBuilder = new TreeBuilder('parameters');

        $rootNode = $treeBuilder->getRootNode();
        $rootNode
            ->ignoreExtraKeys(false)
            ->children()
                ->arrayNode('db')
                    ->children()
                        ->scalarNode('driver')->end()
                        ->scalarNode('host')
                            ->isRequired()
                            ->cannotBeEmpty()
                        ->end()
                        ->scalarNode('port')->end()
                        ->scalarNode('warehouse')->end()
                        ->scalarNode('database')
                            ->isRequired()
                            ->cannotBeEmpty()
                        ->end()
                        ->scalarNode('schema')
                            ->isRequired()
                            ->cannotBeEmpty()
                        ->end()
                        ->scalarNode('user')
                            ->isRequired()
                            ->cannotBeEmpty()
                        ->end()
                        ->scalarNode('password')->end()
                        ->scalarNode('#password')->end()
                    ->end()
                ->end()
            ->end()
        ;

        return $treeBuilder;
    }
}
