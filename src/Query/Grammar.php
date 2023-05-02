<?php

namespace DanielHe4rt\Scylloquent\Query;

use Illuminate\Database\Query\Grammars\Grammar as BaseGrammar;

class Grammar extends BaseGrammar
{
    /**
     * The components that make up a select clause.
     *
     * @var array
     */
    protected $selectComponents = [
        'aggregate',
        'columns',
        'from',
        'wheres',
        'orders',
        'limit',
        'groups',
        'allowFiltering'
    ];

    public function compileAllowFiltering(Builder $query, $bool)
    {
        return (bool) $bool ? 'ALLOW FILTERING' : '';
    }

    public function compileTtl(Builder $query, $ttl)
    {
        return (int) $ttl ? 'using ttl ' . (int) $ttl : '';
    }

    /**
     * Compile an insert statement into SQL.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array  $values
     * @param  array  $options
     * @return string
     */
    public function compileInsertWithOptions(Builder $query, array $values, array $options = [])
    {
        // Essentially we will force every insert to be treated as a batch insert which
        // simply makes creating the SQL easier for us since we can utilize the same
        // basic routine regardless of an amount of records given to us to insert.
        $table = $this->wrapTable($query->from);

        if (empty($values)) {
            return "insert into {$table} default values";
        }

        if (! is_array(reset($values))) {
            $values = [$values];
        }

        $columns = $this->columnize(array_keys(reset($values)));

        // We need to build a list of parameter place-holders of values that are bound
        // to the query. Each insert should have the exact same number of parameter
        // bindings so we will loop through the record and parameterize them all.
        $parameters = collect($values)->map(function ($record) {
            return '('.$this->parameterize($record).')';
        })->implode(', ');

        $queryWith = '';

        if (!empty($options['ttl'])) {
            $queryWith .= $this->compileTtl($query, $options['ttl']);
        }

        return "insert into $table ($columns) values $parameters $queryWith";
    }

    /**
     * Compile an update statement into SQL.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array  $values
     * @param  array  $options
     * @return string
     */
    public function compileUpdateWithOptions(Builder $query, array $values, array $options = [])
    {
        $table = $this->wrapTable($query->from);

        $columns = $this->compileUpdateColumns($query, $values);

        $where = $this->compileWheres($query);

        $compileUpdateWithoutJoins = empty($options['ttl'])
            ? $this->grammar->compileUpdateWithoutJoins($query, $table, $columns, $where)
            : $this->grammar->compileUpdateWithoutJoinsWithOptions(
                $query,
                $table,
                $columns,
                $where,
                ['ttl' => $options['ttl']]
            );

        return trim(
            isset($query->joins)
                ? $this->compileUpdateWithJoins($query, $table, $columns, $where)
                : $compileUpdateWithoutJoins
        );
    }

    /**
     * Compile an update statement without joins into SQL.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  string  $table
     * @param  string  $columns
     * @param  string  $where
     * @param  array   $options
     *
     * @return string
     */
    protected function compileUpdateWithoutJoinsWithOptions(Builder $query, $table, $columns, $where, $options = [])
    {
        $queryWith = '';

        if (!empty($options['ttl'])) {
            $queryWith .= $this->compileTtl($query, $options['ttl']);
        }

        return "update {$table} $queryWith set {$columns} {$where}";
    }
}
