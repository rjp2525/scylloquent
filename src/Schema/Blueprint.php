<?php

namespace DanielHe4rt\Scylloquent\Schema;

use Closure;
use DanielHe4rt\Scylloquent\Exceptions\ScylloquentRuntimeException;
use DanielHe4rt\Scylloquent\Exceptions\ScylloquentSchemaException;
use Illuminate\Database\Schema\Blueprint as BaseBlueprint;
use Illuminate\Support\Fluent;

class Blueprint extends BaseBlueprint
{
    protected array $primaryKeys = [];

    protected array $clusterKeys = [];

    protected WithOption $withOptions;

    /**
     * Specify the primary key(s) for the table.
     * @param array $columns
     */
    public function primary($columns, mixed $name = null, mixed $algorithm = null): Fluent
    {
        $columns = (array)$columns;
        if (isset($columns[0]) && !is_array($columns[0])) {
            $columns[0] = (array)$columns[0];
        }

        if (count($columns) > 1) {
            $this->clusterKeys = array_slice($columns, 1);
        }

        $this->primaryKeys = $columns[0];

        return $this->createCommand('primary', compact('columns', 'algorithm'));
    }

    /**
     * Set With Options
     */
    public function withOptions(Closure $callback)
    {
        $this->withOptions = new WithOption();
        $callback($this->withOptions);
    }


    /**
     * Compile Primary
     */
    public function compilePrimary(): string
    {
        if (empty($this->primaryKeys)) {
            throw ScylloquentSchemaException::missingPartitionKey($this->table);
        }

        $cql = sprintf(
            'primary key (("%s"), "%s") ',
            implode('", "', $this->primaryKeys),
            implode('", "', $this->clusterKeys)
        );

        return str_replace('), "")', '))', $cql);
    }

    /**
     * Compile With Options
     */
    public function compileWithOptions(): string
    {
        if (empty($this->withOptions)) {
            return '';
        }

        return $this->withOptions->compile();
    }

    /**
     * Create a new ascii column on the table.
     */
    public function ascii(string $column): Fluent
    {
        return $this->addColumn('ascii', $column);
    }

    /**
     * Create a new bigint column on the table.
     */
    public function bigint(string $column): Fluent
    {
        return $this->addColumn('bigint', $column);
    }

    /**
     * Create a new blob column on the table.
     */
    public function blob(string $column): Fluent
    {
        return $this->addColumn('blob', $column);
    }

    /**
     * Create a new boolean column on the table.
     */
    public function boolean($column): Fluent
    {
        return $this->addColumn('boolean', $column);
    }

    /**
     * Create a new counter column on the table.
     */
    public function counter(string $column): Fluent
    {
        return $this->addColumn('counter', $column);
    }

    /**
     * Create a new frozen column on the table.
     */
    public function frozen(string $column): Fluent
    {
        return $this->addColumn('frozen', $column);
    }

    /**
     * Create a new inet column on the table.
     */
    public function inet(string $column): Fluent
    {
        return $this->addColumn('inet', $column);
    }

    /**
     * Create a new int column on the table.
     */
    public function int(string $column): Fluent
    {
        return $this->addColumn('int', $column);
    }

    /**
     * Create an integer column on the table.
     *
     * @param string $column
     * @param bool $autoIncrement
     * @param bool $unsigned
     */
    public function integer($column, $autoIncrement = false, $unsigned = false): Fluent
    {
        if ($autoIncrement) {
            return $this->uuid($column);
        }

        return $this->addColumn('int', $column);
    }


    public function listCollection($column, $collectionType): Fluent
    {
        return $this->addColumn('list', $column, compact('collectionType'));
    }

    /**
     * Create a new map column on the table.
     *
     * @param string $column
     * @param string $collectionType1
     * @param string $collectionType2
     * @return \Illuminate\Support\Fluent
     */
    public function mapCollection($column, $collectionType1, $collectionType2): Fluent
    {
        return $this->addColumn('map', $column, compact('collectionType1', 'collectionType2'));
    }

    /**
     * Create a new set column on the table.
     *
     * @param string $column
     * @param string $collectionType
     * @return \Illuminate\Support\Fluent
     */
    public function setCollection($column, $collectionType)
    {
        return $this->addColumn('set', $column, compact('collectionType'));
    }

    /**
     * Create a new timestamp column on the table.
     *
     * @param string $column
     * @param int $precision
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function timestamp($column, $precision = 0): Fluent
    {
        return $this->addColumn('timestamp', $column, compact('precision'));
    }

    /**
     * Create a new timeuuid column on the table.
     */
    public function timeuuid(string $column): Fluent
    {
        return $this->addColumn('timeuuid', $column);
    }

    /**
     * Create a new tuple column on the table.
     */
    public function tuple(string $column, string $tuple1type, string $tuple2type, string $tuple3type): Fluent
    {
        return $this->addColumn('tuple', $column, compact('tuple1type', 'tuple2type', 'tuple3type'));
    }

    /**
     * Create a new varchar column on the table.
     */
    public function varchar(string $column): Fluent
    {
        return $this->addColumn('varchar', $column);
    }

    /**
     * Create a new varint column on the table.
     */
    public function varInt(string $column): Fluent
    {
        return $this->addColumn('varint', $column);
    }
}