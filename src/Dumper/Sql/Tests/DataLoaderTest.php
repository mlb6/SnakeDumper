<?php

namespace Digilist\SnakeDumper\Dumper\Sql\Tests;

use Digilist\SnakeDumper\Configuration\DatabaseConfiguration;
use Digilist\SnakeDumper\Configuration\SqlDumperConfiguration;
use Digilist\SnakeDumper\Configuration\Table\TableConfiguration;
use Digilist\SnakeDumper\Dumper\Sql\ConnectionHandler;
use Digilist\SnakeDumper\Dumper\Sql\DataLoader;
use Digilist\SnakeDumper\Dumper\Sql\SqlDumperContext;
use Digilist\SnakeDumper\Dumper\Sql\SqlDumperState;
use Digilist\SnakeDumper\Dumper\Sql\TableSelector;
use Digilist\SnakeDumper\Dumper\SqlDumper;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\IntegerType;
use Doctrine\DBAL\Types\StringType;
use Psr\Log\NullLogger;
use Symfony\Component\Console\Input\StringInput;
use Symfony\Component\Console\Output\NullOutput;

class DataLoaderTest extends AbstractSqlTest
{

    /**
     * @var DataLoader
     */
    private $dataLoader;

    /**
     * @var \ReflectionMethod
     */
    private $createSelectQueryBuilder;

    /**
     *
     */
    public function setUp()
    {
        parent::setUp();

        $this->dataLoader = new DataLoader($this->context);

        $refl = new \ReflectionObject($this->dataLoader);
        $createSelectQueryBuilder = $refl->getMethod('createSelectQueryBuilder');
        $createSelectQueryBuilder->setAccessible(true);

        $this->createSelectQueryBuilder = $createSelectQueryBuilder;
    }

    /**
     * Tests whether the standard select query is build correctly.
     *
     * @test
     */
    public function testStandardQuery()
    {
        $table = new Table('`Customer`'); // Table name must be always quoted

        $this->setConfig([
            'tables' => [
                'Customer' => [
                ]
            ]
        ]);
        $this->initState([$table]);

        $query = $this->createSelectQueryBuilder($table)->getSQL();
        $this->assertEquals('SELECT * FROM `Customer` t', $query);
    }

    /**
     * Tests whether a select query with limit is build correctly.
     *
     * @test
     */
    public function testLimit()
    {
        $table = new Table('`Customer`'); // Table name must be always quoted
        $this->setConfig([
            'tables' => [
                'Customer' => [
                    'limit' => 100
                ]
            ]
        ]);
        $this->initState([$table]);

        $query = $this->createSelectQueryBuilder($table)->getSQL();
        $this->assertEquals('SELECT * FROM `Customer` t LIMIT 100', $query);
    }

    /**
     * Tests whether a select query with order by is build correctly.
     *
     * @test
     */
    public function testOrderBy()
    {
        $table = new Table('`Customer`'); // Table name must be always quoted
        $this->setConfig([
            'tables' => [
                'Customer' => [
                    'order_by' => 'id DESC'
                ]
            ]
        ]);
        $this->initState([$table]);

        $query = $this->createSelectQueryBuilder($table)->getSQL();
        $this->assertEquals('SELECT * FROM `Customer` t ORDER BY id DESC', $query);
    }

    /**
     * Tests whether a filter is used correctly.
     * We only test a single filter, as we expect Doctrine Expressions to work correctly.
     *
     * @test
     */
    public function testBasicFilter()
    {
        $table = new Table('`Customer`'); // Table name must be always quoted
        $this->setConfig([
            'tables' => [
                'Customer' => [
                    'filters' => [
                        0 => ['eq', 'id', 1]
                    ]
                ]
            ]
        ]);
        $this->initState([$table]);

        $query = $this->createSelectQueryBuilder($table)->getSQL();

        $expectedQuery = 'SELECT * FROM `Customer` t WHERE `id` = :param_0';
        $this->assertEquals($expectedQuery, $query);
    }

    /**
     * Tests whether the IN filter is build correctly.
     *
     * @test
     */
    public function testInFilter()
    {
        $table = new Table('`Customer`'); // Table name must be always quoted
        $this->setConfig([
            'tables' => [
                'Customer' => [
                    'filters' => [
                        0 => ['in', 'id', [1, 2, 3]]
                    ]
                ]
            ]
        ]);
        $this->initState([$table]);

        $query = $this->createSelectQueryBuilder($table)->getSQL();

        $expectedQuery = 'SELECT * FROM `Customer` t WHERE `id` IN (:param_0_0, :param_0_1, :param_0_2)';
        $this->assertEquals($expectedQuery, $query);
    }


    /**
     * Tests whether the isNotNull is built correctly.
     *
     * @test
     */
    public function testNotNullFilter()
    {
        $table = new Table('`Table`'); // Table name must be always quoted
        $this->setConfig([
            'tables' => [
                'Table' => [
                    'filters' => [
                        0 => ['isNotNull', 'column']
                    ]
                ]
            ]
        ]);
        $this->initState([$table]);

        $query = $this->createSelectQueryBuilder($table)->getSQL();

        $expectedQuery = 'SELECT * FROM `Table` t WHERE `column` IS NOT NULL';
        $this->assertEquals($expectedQuery, $query);
    }

    /**
     * Tests whether the isNull is built correctly.
     *
     * @test
     */
    public function testNullFilter()
    {
        $table = new Table('`Table`'); // Table name must be always quoted
        $this->setConfig([
            'tables' => [
                'Table' => [
                    'filters' => [
                        0 => ['isNull', 'column']
                    ]
                ]
            ]
        ]);
        $this->initState([$table]);

        $query = $this->createSelectQueryBuilder($table)->getSQL();

        $expectedQuery = 'SELECT * FROM `Table` t WHERE `column` IS NULL';
        $this->assertEquals($expectedQuery, $query);
    }

    /**
     * Tests whether multiple filters will be AND-ed correctly
     *
     * @test
     */
    public function testMultipleFilter()
    {
        $table = new Table('`Customer`'); // Table name must be always quoted
        $this->setConfig([
            'tables' => [
                'Customer' => [
                    'filters' => [
                        0 => ['lt', 'id', 100],
                        1 => ['eq', 'name', 'Markus']
                    ]
                ]
            ]
        ]);
        $this->initState([$table]);

        $query = $this->createSelectQueryBuilder($table)->getSQL();

        $expectedQuery = 'SELECT * FROM `Customer` t WHERE (`id` < :param_0) AND (`name` = :param_1)';
        $this->assertEquals($expectedQuery, $query);
    }

    /**
     * Tests whether multiple filters will be AND-ed correclty
     *
     * @test
     */
    public function testDataDependentFilter()
    {
        $billingTable = new Table('`Billing`',  [new Column('customer_id', new IntegerType())] ); // Table name must be always quoted
        $this->setConfig([
            'tables' => [
                'Billing' => [
                    'filters' => [
                        0 => [ 'depends', 'customer_id', 'Customer.id'],
                    ]
                ]
            ]
        ]);
        $this->initState([$billingTable, new Table('Customer', [new Column('id', new IntegerType())])]);


        foreach ([10, 11, 12, 13] as $value) {
            $this->context->getDumperState()->addHarvestedValue('Customer', 'id', $value);
        }


        $query = $this->createSelectQueryBuilder($billingTable)->getSQL();

        $expectedQuery = 'SELECT * FROM `Billing` t WHERE (customer_id IN (:param_0_0, :param_0_1, :param_0_2, :param_0_3)) OR (`customer_id` IS NULL)';
        $this->assertEquals($expectedQuery, $query);
    }


    /**
     * Tests whether the Composite filters are built correctly.
     *
     * @test
     */
    public function testCompositeFilters()
    {
        $table = new Table('`Table`');
        $this->setConfig([
            'tables' => [
                'Table' => [
                    'filters' => [
                        ['or', ['eq', 'col1', 1], ['eq', 'col2', 2] ,['eq', 'col3', 3]],
                        ['and', ['gt', 'col1', 0], ['gt', 'col2', 2] ,['gt', 'col3', 3]]
                    ]
                ]
            ]
        ]);
        $this->initState([$table]);


        $query = $this->createSelectQueryBuilder($table)->getSQL();

        $expectedQuery = 'SELECT * FROM `Table` t WHERE ((`col1` = :param_0) OR (`col2` = :param_1) '
            .'OR (`col3` = :param_2)) AND ((`col1` > :param_3) AND (`col2` > :param_4) AND (`col3` > :param_5))';
        $this->assertEquals($expectedQuery, $query);
    }


    /**
     * Tests whether a table is white listed works correctly.
     */
    public function testRegularDependency()
    {
        $table1 =  new Table('`Table1`',  [
            new Column('id', new IntegerType()),
            new Column('ref_id', new IntegerType())
        ] );
        $table2 =  new Table('`Table2`',  [
            new Column('id', new IntegerType())
        ] );

        $this->setConfig([
            'tables' => [
                'Table1' => [
                    'dependencies' => [
                        [
                            'column' => 'ref_id',
                            'referenced_table' => 'Table2',
                            'referenced_column' => 'id',
                            'condition' => ['eq', 'ref_table', 'Table2']
                        ]
                    ]
                ]
            ]
        ]);
        $this->initState([$table1, $table2]);
        foreach ([1,2,3] as $value) {
            $this->context->getDumperState()->addHarvestedValue('Table2', 'id', $value);
        }

        $query = $this->createSelectQueryBuilder($table1)->getSQL();
        $expectedQuery = 'SELECT * FROM `Table1` t WHERE '
            .'((ref_id IN (:param_0_0, :param_0_1, :param_0_2)) OR (`ref_id` IS NULL)) AND (`ref_table` = :param_1)';
        $this->assertEquals($expectedQuery, $query);
    }

    /**
     * Tests whether a table is white listed works correctly.
     */
    public function testDoubleDependencyOnSameColumn()
    {
        $table1 =  new Table('`Table1`',  [
            new Column('id', new IntegerType()),
            new Column('ref_id', new IntegerType()),
            new Column('ref_table', new StringType())
        ] );
        $table2 =  new Table('`Table2`',  [
            new Column('id', new IntegerType())
        ] );
        $table3 =  new Table('`Table3`',  [
            new Column('id', new IntegerType())
        ] );

        $this->setConfig([
            'tables' => [
                'Table1' => [
                    'dependencies' => [
                        [
                            'column' => 'ref_id',
                            'referenced_table' => 'Table2',
                            'referenced_column' => 'id',
                            'condition' => ['eq', 'ref_table', 'Table2']
                        ],
                        [
                            'column' => 'ref_id',
                            'referenced_table' => 'Table3',
                            'referenced_column' => 'id',
                            'condition' => ['eq', 'ref_table', 'Table3']
                        ]
                    ]
                ]
            ]
        ]);

        $this->initState([$table1, $table2, $table3]);
        foreach ([1,2,3] as $value) {
            $this->context->getDumperState()->addHarvestedValue('Table2', 'id', $value);
        }
        foreach ([1] as $value) {
            $this->context->getDumperState()->addHarvestedValue('Table3', 'id', $value);
        }

        $query = $this->createSelectQueryBuilder($table1)->getSQL();
        $expectedQuery = 'SELECT * FROM `Table1` t WHERE '
            .'('
                .'((ref_id IN (:param_0_0, :param_0_1, :param_0_2)) OR (`ref_id` IS NULL)) AND (`ref_table` = :param_1)'
            .') '
            .'OR '
            .'('
                .'((ref_id IN (:param_2_0)) OR (`ref_id` IS NULL)) AND (`ref_table` = :param_3)'
            .')';
        $this->assertEquals($expectedQuery, $query);
    }


    /**
     * Tests whether a table is white listed works correctly.
     */
    public function testColumnAsReferencedTable()
    {
        $this->createTestDependenciesSchema();
        $this->setConfig([
            'tables' => [
                'Customer' => [
                    'limit' => 4,
                ],
                'BadgeMembership' => [
                    'dependencies' => [
                        [
                            'column' => 'item_id',
                            'column_as_referenced_table' => 'item_table',
                            'referenced_column' => 'id'
                        ]
                    ]
                ],
            ],
        ]);

        $tableFinder = new TableSelector($this->context->getConnectionHandler()->getConnection(), $this->context->getLogger());
        $tables = $tableFinder->findTablesToDump($this->context->getConfig());
        $this->context->setDumperState(new SqlDumperState($tables));
        $this->context->getConfig()->hydrateConfig($this->dataLoader);
        $badgeMembershipTable = $this->context->getDumperState()->getTableByName('BadgeMembership');


        foreach ([1,2,3,4] as $value) {
            $this->context->getDumperState()->addHarvestedValue('Customer', 'id', $value);
        }
        foreach ([1,2] as $value) {
            $this->context->getDumperState()->addHarvestedValue('SKU', 'id', $value);
        }

        $query = $this->createSelectQueryBuilder($badgeMembershipTable);
        $sql = $query->getSQL();
        $parameters = $query->getParameters();

        $expectedSQL = 'SELECT * FROM `BadgeMembership` t WHERE '
            .'('
                .'((`item_id` IN (:param_0_0, :param_0_1, :param_0_2, :param_0_3)) OR (`item_id` IS NULL)) AND (`item_table` = :param_1)'
            .') '
            .'OR '
            .'('
                .'((`item_id` IN (:param_2_0, :param_2_1)) OR (`item_id` IS NULL)) AND (`item_table` = :param_3)'
            .')';
        $expectedParameters = [
            'param_0_0' => 1,
            'param_0_1' => 2,
            'param_0_2' => 3,
            'param_0_3' => 4,
            'param_1' => 'Customer',
            'param_2_0' => 1,
            'param_2_1' => 2,
            'param_3' => 'SKU',
        ];
        $this->assertEquals($expectedSQL, $sql);
        $this->assertEquals($expectedParameters, $parameters);
    }


    /**
     * Tests whether a table is white listed works correctly.
     */
    public function test1()
    {
        $this->createTestDependencies2Schema();
        $this->setConfig([
            'tables' => [
                'Customer' => [
                    'limit' => 1,
                ],
                'ActivityDetail' => [
                    'dependencies' => [
                        [
                            'column' => 'id',
                            'referenced_table' => 'ActivityLog',
                            'referenced_column' => 'activity_id'
                        ]
                    ]
                ],
            ],
        ]);

        $tableFinder = new TableSelector($this->context->getConnectionHandler()->getConnection(), $this->context->getLogger());
        $tables = $tableFinder->findTablesToDump($this->context->getConfig());
        $this->context->setDumperState(new SqlDumperState($tables));
        $this->context->getConfig()->hydrateConfig($this->dataLoader);

        $activityDetailTable = $this->context->getDumperState()->getTableByName('ActivityDetail');

        foreach ([1] as $value) {
            $this->context->getDumperState()->addHarvestedValue('Customer', 'id', $value);
        }

        $query = $this->createSelectQueryBuilder($activityDetailTable);
        $sql = $query->getSQL();
        $parameters = $query->getParameters();

        $expectedSQL = 'SELECT * FROM `ActivityDetail` t WHERE (`id` IN (:param_1_0, :param_1_1, :param_1_2)) OR (`id` IS NULL)';
        $expectedParameters = [
            'param_0_0' => 1,
            'param_0_1' => 2,
            'param_0_2' => 6,
        ];
        $this->assertEquals($expectedSQL, $sql);
        $this->assertEquals($expectedParameters, $parameters);
    }


    /**
     * @param TableConfiguration $tableConfig
     * @param Table              $table
     * @param array              $harvestedValues
     * @return QueryBuilder
     */
    private function createSelectQueryBuilder(Table $table)
    {
        return $this->createSelectQueryBuilder->invoke($this->dataLoader, $table);
    }
}
