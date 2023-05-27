<?php

namespace DanielHe4rt\Scylloquent\Schema;

use Cassandra\Rows;
use Closure;
use DanielHe4rt\Scylloquent\Connection;
use Illuminate\Database\Schema\Builder as BaseBuilder;

class Builder extends BaseBuilder
{
    /** @var Connection */
    protected $connection;

    /** @var Grammar */
    protected $grammar;

    /**
     * Determine if the given table exists.
     */
    public function hasTable($table): bool
    {
        $table = $this->connection->getTablePrefix() . $table;
        $keyspace = $this->connection->getKeyspace();

        $args = ['table_name' => $table, 'keyspace_name' => $keyspace];

        /** @var Rows $rows */
        $rows = $this->connection->selectFromWriteConnection($this->grammar->compileTableExists(), $args);

        return $rows->count() > 0;
    }

    /**
     * Create a new command set with a Closure.
     */
    protected function createBlueprint($table, Closure $callback = null): Blueprint
    {
        $prefix = $this->connection->getConfig('prefix_indexes')
            ? $this->connection->getConfig('prefix')
            : '';

        if (isset($this->resolver)) {
            return call_user_func($this->resolver, $table, $callback, $prefix);
        }

        return new Blueprint($table, $callback, $prefix);
    }

    /**
     * Drop all tables from the database.
     */
    public function dropAllTables(): void
    {
        $tables = [];

        foreach ($this->getAllTables() as $row) {
            $row = (array)$row;

            $tables[] = reset($row);
        }

        if (empty($tables)) {
            return;
        }

        foreach ($tables as $table) {
            $this->connection->statement(
                $this->grammar->compileDropTableIfExists($table)
            );
        }
    }

    /**
     * Get all the table names for the database.
     */
    public function getAllTables(): Rows
    {
        return $this->connection->select(
            $this->grammar->compileGetAllTables(),
            ['keyspace_name' => $this->connection->getKeyspace()]
        );
    }
}
