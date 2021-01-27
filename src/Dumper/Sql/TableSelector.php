<?php

namespace Digilist\SnakeDumper\Dumper\Sql;

use Digilist\SnakeDumper\Configuration\DumperConfigurationInterface;
use Digilist\SnakeDumper\Configuration\Table\TableConfiguration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Schema\Table;
use Psr\Log\LoggerInterface;

/**
 * This class helps to find the tables that should be dumped.
 *
 * It identifies all tables in the database and excludes those which have been ignored in the configuration.
 * Furthermore, it identifies dependencies between tables (through foreign keys) and adds those dependencies
 * into the configuration.
 */
class TableSelector
{

    /**
     * @var Connection
     */
    private $connection;

    /**
     * @var IdentifierQuoter
     */
    private $identifierQuoter;

    /**
     * @var TableDependencyResolver
     */
    private $tableDependencyResolver;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @param Connection $connection
     * @param LoggerInterface $logger
     */
    public function __construct(Connection $connection, LoggerInterface $logger)
    {
        $this->logger = $logger;
        $this->connection = $connection;
        $this->identifierQuoter = new IdentifierQuoter($connection);
        $this->tableDependencyResolver = new TableDependencyResolver();
    }

    /**
     * Returns an array with all available tables that should be dumped. Those tables are pre-sorted according to
     * their dependencies to other tables. Furthermore, the identifiers of the tables are already quoted.
     *
     * @param DumperConfigurationInterface $config
     *
     * @return \Doctrine\DBAL\Schema\Table[]
     */
    public function findTablesToDump(DumperConfigurationInterface $config)
    {
        $schemaManager = $this->connection->getSchemaManager();
        $this->logger->debug('Listing all tables from SQL DB.');
        $tables = $schemaManager->listTables();
        $this->logger->debug('DONE');
        $this->createMissingTableConfigs($config, $tables);

        $filter = new TableFilter($config);
        $tables = $filter->filterWhiteListTables($tables);
        $tables = $filter->filterIgnoredTables($tables);

        $tables = $this->tableDependencyResolver->sortTablesByDependencies($tables, $this->logger);
        $this->tableDependencyResolver->createDependentFilters($tables, $config);

        // Quote all identifiers, as Doctrine DBAL only quotes reserved keywords by default
        $tables = $this->identifierQuoter->quoteTables($tables);

        return $tables;
    }

    /**
     * Create table configurations for all tables that were not configured yet.
     *
     * @param DumperConfigurationInterface $config
     * @param Table[]                      $tables
     */
    private function createMissingTableConfigs(DumperConfigurationInterface $config, array $tables)
    {
        foreach ($tables as $table) {
            if (!$config->hasTableConfig($table->getName())) {
                $config->addTableConfig(new TableConfiguration($table->getName()));
            }
        }
    }
}
