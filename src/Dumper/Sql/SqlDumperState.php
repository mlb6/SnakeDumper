<?php


namespace Digilist\SnakeDumper\Dumper\Sql;


use Doctrine\DBAL\Schema\Table;

class SqlDumperState
{

    protected $harvestedValues = [];
    protected $tablesByName = [];
    protected $queryState;


    public function __construct(array $tables)
    {
        $this->tablesByName = array_reduce($tables, function ($acc, Table $table) {
            $acc[$table->getName()] = $table;
            return $acc;
        }, []);
        $this->queryState = new SqlQueryState();
    }


    /**
     * @param array $tables
     */
    public function setTables(array $tables) {

    }

    /**
     * @param string $tableName
     * @return Table|null
     */
    public function getTableByName($tableName)
    {
        return isset($this->tablesByName[$tableName]) ? $this->tablesByName[$tableName] : null;
    }


    public function isTableHarvested($tableName)
    {
        return isset($this->harvestedValues[$tableName]);
    }


    public function getHarvestedValues($tableName, $column)
    {
        if (!isset($this->harvestedValues[$tableName]) || !isset($this->harvestedValues[$tableName][$column])) {
            return null;
        }
        return $this->harvestedValues[$tableName][$column];
    }

    public function addHarvestedValue($tableName, $column, $value)
    {
        if (!isset($this->harvestedValues[$tableName])) {
            $this->harvestedValues[$tableName] = [];
        }
        if (!isset($this->harvestedValues[$tableName][$column])) {
            $this->harvestedValues[$tableName][$column] = [];
        }
        if ($value) {
            $this->harvestedValues[$tableName][$column][] = $value;
        }
    }


    /**
     * @return SqlQueryState
     */
    public function getCurrentQueryState()
    {
        return $this->queryState;
    }
}