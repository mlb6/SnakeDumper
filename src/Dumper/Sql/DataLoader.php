<?php

namespace Digilist\SnakeDumper\Dumper\Sql;

use Digilist\SnakeDumper\Configuration\Table\Filter\DataDependentFilter;
use Digilist\SnakeDumper\Configuration\Table\Filter\ColumnFilter;
use Digilist\SnakeDumper\Configuration\Table\Filter\CompositeFilter;
use Digilist\SnakeDumper\Configuration\Table\Filter\FilterInterface;
use Digilist\SnakeDumper\Configuration\Table\TableConfiguration;
use Digilist\SnakeDumper\Dumper\DataLoaderInterface;
use Digilist\SnakeDumper\Exception\UnsupportedFilterException;
use Doctrine\DBAL\Query\Expression\CompositeExpression;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Schema\Table;
use InvalidArgumentException;
use Psr\Log\LoggerInterface;

/**
 * This class helps to query the appropriate data that should be dumped.
 */
class DataLoader implements DataLoaderInterface
{

    /**
     * @var ConnectionHandler
     */
    private $connectionHandler;
    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var SqlDumperContext
     */
    private $context;

    /**
     * @param ConnectionHandler $connectionHandler
     * @param LoggerInterface   $logger
     */
    public function __construct(SqlDumperContext $context)
    {
        $this->context = $context;
        $this->connectionHandler = $context->getConnectionHandler();
        $this->logger = $context->getLogger();
    }

    /**
     * @return SqlDumperState
     */
    public function getDumperState()
    {
        return $this->context->getDumperState();
    }

    /**
     * @return SqlQueryState
     */
    public function getCurrentQueryState()
    {
        return $this->getDumperState()->getCurrentQueryState();
    }

    /**
     * Executes the generated sql statement.
     *
     * @param Table $table
     * @return \Doctrine\DBAL\Driver\Statement
     * @throws UnsupportedFilterException
     * @throws \Doctrine\DBAL\DBALException
     */
    public function executeSelectQuery(Table $table)
    {
        list($query, $parameters) = $this->buildSelectQuery($table);

        $this->logger->debug('Executing select query: ' . $query);
        $result = $this->connectionHandler->getConnection()->prepare($query);
        $result->execute($parameters);

        return $result;
    }

    /**
     * Count the number of rows for the generated select statement.
     *
     * @param Table $table
     * @param SqlDumperContext $dumperState
     * @return int
     * @throws UnsupportedFilterException
     * @throws \Doctrine\DBAL\DBALException
     */
    public function countRows(Table $table)
    {
        list($query, $parameters) = $this->buildSelectQuery($table);

        // Remove everything before the first FROM, to replace it with a SELECT 1
        $query = 'SELECT 1 ' . substr($query, stripos($query, 'FROM'));

        // The actual select is wrapped in a subquery, to consider groupings, limits etc.
        // We only want to get the number of rows that will be dumped later.
        $query = sprintf('SELECT COUNT(*) FROM (%s) AS tmp', $query);

        $result = $this->connectionHandler->getConnection()->prepare($query);
        $result->execute($parameters);

        return (int) $result->fetchAll()[0]['COUNT(*)'];
    }

    /**
     * This method creates the actual select statements and binds the parameters.
     *
     * @param Table $table
     * @param SqlDumperStateInterface $dumperState
     * @param null $columnName
     * @return array
     * @throws UnsupportedFilterException
     * @throws \Doctrine\DBAL\DBALException
     */
    private function buildSelectQuery(Table $table, $columnName = null)
    {
        $this->getCurrentQueryState()->reset();
        $qb = $this->createSelectQueryBuilder($table, $columnName);

        $query = $qb->getSQL();
        $parameters = $qb->getParameters();

        $tableConfig = $this->context->getTableConfig($table);
        if ($tableConfig->getQuery() != null) {
            $query = $tableConfig->getQuery();

            // Add automatic conditions to the custom query if necessary
            $parameters = [];
            if (strpos($query, '$autoConditions') !== false) {
                $parameters = $qb->getParameters();
                $query = str_replace('$autoConditions', '(' . $qb->getQueryPart('where') . ')', $query);
            }
        }

        return [trim($query), $parameters];
    }

    /**
     * @param Table $table
     * @param SqlDumperStateInterface $dumperState
     * @param null $columnName
     * @return QueryBuilder
     * @throws UnsupportedFilterException
     * @throws \Doctrine\DBAL\DBALException
     */
    private function createSelectQueryBuilder(Table $table, $columnName = null)
    {
        $platform = $this->connectionHandler->getPlatform();
        $qb = $this->connectionHandler->getConnection()->createQueryBuilder()
            ->select($columnName ? $table->getColumn($columnName)->getQuotedName($platform) : '*')
            ->from($table->getQuotedName($platform), 't');

        if ($columnName) {
            $qb->distinct();
        }

        $tableConfig = $this->context->getTableConfig($table);
        $this->addFiltersToSelectQuery($qb, $tableConfig);
        if ($tableConfig->getLimit() != null) {
            $qb->setMaxResults($tableConfig->getLimit());
        }
        if ($tableConfig->getOrderBy() != null) {
            $qb->add('orderBy', $tableConfig->getOrderBy());
        }

        return $qb;
    }

    /**
     * Add the configured filter to the select query.
     *
     * @param QueryBuilder $qb
     * @param TableConfiguration $tableConfig
     * @param SqlDumperStateInterface $dumperState
     * @throws UnsupportedFilterException
     */
    private function addFiltersToSelectQuery(QueryBuilder $qb, TableConfiguration $tableConfig)
    {
        foreach ($tableConfig->getFilters() as $filter) {
            $expr = $this->addFilterToSelectQuery($qb, $tableConfig,  $filter);
            $qb->andWhere($expr);
        }
    }

    /**
     * @param QueryBuilder $qb
     * @param TableConfiguration $tableConfig
     * @param array $harvestedValues
     * @param $filter
     * @param $paramIndex
     * @return CompositeExpression|mixed
     * @throws UnsupportedFilterException
     */
    private function addFilterToSelectQuery(QueryBuilder $qb, TableConfiguration $tableConfig, $filter)
    {
        if ($filter instanceof ColumnFilter) {
            if ($filter instanceof DataDependentFilter) {
                $expr = $this->handleDataDependentFilter($qb,  $tableConfig, $filter);
            } else {
                $param = $this->bindParameters($qb, $filter);
                $expr = call_user_func_array([$qb->expr(), $filter->getOperator()], [
                    $this->connectionHandler->getPlatform()->quoteIdentifier($filter->getColumnName()),
                    $param
                ]);
            }
            return $expr;
        }

        if ($filter instanceof CompositeFilter) {
            $filter->getFilters();
            return call_user_func_array(
                [$qb->expr(), $filter->getOperator()],
                array_map(
                    function ($childFilter) use ($qb, $tableConfig) {
                        return $this->addFilterToSelectQuery($qb, $tableConfig, $childFilter);
                    },
                    $filter->getFilters()
                )
            );

        }
        throw new UnsupportedFilterException();
    }

    /**
     * Validates and modifies the data dependent filter to act like an IN-filter.
     *
     * @param QueryBuilder $qb
     * @param TableConfiguration $tableConfig
     * @param array $harvestedValues
     * @param DataDependentFilter $filter
     * @param $paramIndex
     * @return CompositeExpression
     */
    private function handleDataDependentFilter(
        QueryBuilder $qb,
        TableConfiguration $tableConfig,
        DataDependentFilter $filter
    ) {
        $platform = $this->connectionHandler->getPlatform();
        $tableName = $tableConfig->getName();
        $table = $this->getDumperState()->getTableByName($tableName);
        $column = $table->getColumn($filter->getColumnName());
        $referencedTable = $filter->getReferencedTable();
        $referencedColumn = $filter->getReferencedColumn();

        $harvestedValues = $this->getDumperState()->getHarvestedValues($referencedTable, $referencedColumn);

        // If table has been dumped before the current table: Ensure the necessary column was included in the dump
        if ($this->getDumperState()->isTableHarvested($referencedTable) && is_null($harvestedValues)) {
            throw new InvalidArgumentException(
                sprintf(
                    'The %s column of table %s has not been dumped. (dependency of %s)',
                    $referencedColumn,
                    $referencedTable,
                    $tableName
                )
            );
        }

        if (is_null($harvestedValues)) {
            $queryState = $this->getCurrentQueryState();
            if (!$queryState->containsDependency($referencedTable)) {
                $queryState->pushDependency($tableName);
                list($query, $parameters) = $this->buildSelectQuery($this->getDumperState()->getTableByName($referencedTable), $referencedColumn);
                $queryState->popDependency();
                $this->logger->debug("Harvesting $referencedTable:$referencedColumn for $tableName with  query: " . $query);
                $results = $this->connectionHandler->getConnection()->prepare($query);
                $results->execute($parameters);
                $results = array_unique(array_map(function ($row) use ($referencedColumn) {
                    return is_numeric($row[$referencedColumn]) ? intval($row[$referencedColumn]) : $row[$referencedColumn];
                }, iterator_to_array($results)));
                $this->logger->debug("Harvest results : " . count($results));
                $harvestedValues = $results;
            } else {
                $this->logger->warning('Skip dependency filter for : ' . $referencedTable);
                return '';
            }
        }




        if (!is_null($harvestedValues)) {
            $filter->setValue($harvestedValues);

            $param = $this->bindParameters($qb, $filter);
            $expr = $qb->expr()->in(
                $column->getQuotedName($platform),
                $param
            );
        } else {
            $this->logger->debug('RETURN no expression');
            return '';
        }

        // also select null values
        return $qb->expr()->orX(
            $expr,
            $qb->expr()->isNull(
                $this->connectionHandler->getPlatform()->quoteIdentifier($filter->getColumnName())
            )
        );


    }

    /**
     * Binds the parameters of the filter into the query builder.
     *
     * This function returns false, if the condition is not fulfill-able and no row can be selected at all.
     *
     * @param QueryBuilder    $qb
     * @param FilterInterface $filter
     * @param int             $paramIndex
     *
     * @return array|string|bool
     */
    private function bindParameters(QueryBuilder $qb, FilterInterface $filter)
    {
        if(in_array($filter->getOperator(), [
            ColumnFilter::OPERATOR_IS_NOT_NULL,
            ColumnFilter::OPERATOR_IS_NULL,
        ])) {
            return;
        };

        $paramIndex = $this->getCurrentQueryState()->incrementParamIndex();

        $inOperator = in_array($filter->getOperator(), [
            ColumnFilter::OPERATOR_IN,
            ColumnFilter::OPERATOR_NOT_IN,
        ]);

        if ($inOperator) {
            // the IN and NOT IN operator expects an array which needs a different handling
            // -> each value in the array must be mapped to a single param

            $values = (array) $filter->getValue();
            if (empty($values)) {
                $values = array('_________UNDEFINED__________');
            }

            $param = array();
            foreach ($values as $valueIndex => $value) {
                $tmpParam = 'param_' . $paramIndex . '_' . $valueIndex;
                $param[] = ':' . $tmpParam;

                $qb->setParameter($tmpParam, $value);
            }
        } else {
            $param = ':param_' . $paramIndex;

            $qb->setParameter('param_' . $paramIndex, $filter->getValue());
        }

        return $param;
    }

    /**
     * Get distinct values from a table and column
     *
     * @param string $table
     * @param string $property
     * @return \Traversable
     * @throws \Doctrine\DBAL\DBALException
     */
    public function getDistinctValues($tableName, $property)
    {
        $connection = $this->connectionHandler->getConnection();
        $table = $connection->getSchemaManager()->listTableDetails($tableName);
        $qb = $connection->createQueryBuilder()
            ->select($property)->distinct()
            ->from($table->getQuotedName($this->connectionHandler->getPlatform()), 't');

        $query = $qb->getSQL();
        $parameters = $qb->getParameters();

        $this->logger->debug('Executing select query: ' . $query);
        $results = $this->connectionHandler->getConnection()->prepare($query);
        $results->execute($parameters);
        $results = array_map(function ($row) use ($property) {
            return $row[$property];
        }, iterator_to_array($results));
        return $results;
    }

    /**
     * Verify if the given table name exist
     *
     * @param string $tableName
     * @return bool
     */
    public function isExistingTable($tableName)
    {
        return !is_null($this->getDumperState()->getTableByName($tableName));
    }
}
