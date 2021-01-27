<?php


namespace Digilist\SnakeDumper\Dumper\Sql;


class SqlQueryState
{
    protected $queryDependencies = [];
    protected $paramIndex = 0;

    /**
     * @param $tableName
     * @return void
     */
    public function pushDependency($tableName)
    {
        array_push($this->queryDependencies, $tableName);
    }

    /**
     * @return void
     */
    public function popDependency()
    {
        array_pop($this->queryDependencies);
    }

    /**
     * @param $tableName
     * @return bool
     */
    public function containsDependency($tableName)
    {
        return in_array($tableName, $this->queryDependencies);
    }

    /**
     * @return int
     */
    public function incrementParamIndex()
    {
        return $this->paramIndex++;
    }

    /**
     * @return void
     */
    public function reset()
    {
        $this->paramIndex = 0;
    }
}