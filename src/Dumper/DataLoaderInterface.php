<?php


namespace Digilist\SnakeDumper\Dumper;


interface DataLoaderInterface
{
    /**
     * Get distinct values from a table and column
     *
     * @param string $table
     * @param string $property
     * @return array
     */
    public function getDistinctValues($table, $property);

    /**
     * Verify if the given table name exist
     *
     * @param string $tableName
     * @return bool
     */
    public function isExistingTable($tableName);
}