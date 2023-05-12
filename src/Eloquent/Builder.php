<?php

namespace DanielHe4rt\Scylloquent\Eloquent;

use Cassandra\Rows;
use DanielHe4rt\Scylloquent\Collection;
use Illuminate\Database\Eloquent\Builder as EloquentBuilder;

class Builder extends EloquentBuilder
{
    /**
     * Create a collection of models from plain arrays.
     *
     * @param  \Cassandra\Rows|array  $rows
     *
     * @return Collection
     */
    public function hydrateRows(Rows|array $rows): Collection
    {
        /** @var Model $instance */
        $instance = $this->newModelInstance();

        return $instance->newCassandraCollection($rows);
    }

    /**
     * Execute the query as a "select" statement.
     *
     * @param  array  $columns
     *
     * @return Collection
     *
     * @throws \Exception
     */
    public function getPage($columns = ['*']): Collection|Rows
    {
        $builder = $this->applyScopes();

        return $builder->getModelsPage($columns);
    }

    /**
     * Get the hydrated models without eager loading.
     *
     * @param  array  $columns
     *
     * @return Collection
     *
     * @throws \Exception
     */
    public function getModelsPage($columns = ['*'])
    {
        $results = $this->getPage($columns);

        if ($results instanceof Collection) {
            $rows = $results->getRows();
            $results = $rows->isLastPage() ? $results->all() : $rows;

        } elseif (!$results instanceof Rows) {
            throw new \Exception('Invalid type of getPage response. Expected lroman242\LaravelCassandra\Collection or Cassandra\Rows');
        }

        return $this->hydrateRows($results);
    }

    /**
     * Execute the query as a "select" statement.
     *
     * @param array $columns
     *
     * @throws \Exception
     */
    public function get($columns = ['*']): array|\Illuminate\Database\Eloquent\Collection|Collection
    {
        $builder = $this->applyScopes();

        return $builder->getModels($columns);
    }

    /**
     * Get the hydrated models without eager loading.
     *
     * @param  array  $columns
     *
     * @throws \Exception
     */
    public function getModels($columns = ['*']): Collection
    {
        $results = $this->query->get($columns);

        if ($results instanceof Collection) {
            $rows = $results->getRows();
            $results = $rows->isLastPage() ? $results->all() : $rows;
        } elseif (!$results instanceof Rows) {
            throw new \Exception('Invalid type of getPage response. Expected DanielHe4rt\Scylloquent\Collection or Cassandra\Rows');
        }

        return $this->hydrateRows($results);
    }

    /**
     * Add the "updated at" column to an array of values.
     *
     * @param  array  $values
     * @return array
     */
    protected function addUpdatedAtColumn(array $values): array
    {
        if (!$this->model->usesTimestamps() || is_null($this->model->getUpdatedAtColumn())) {
            return $values;
        }

        $column = $this->model->getUpdatedAtColumn();

        $values = array_merge(
            [$column => $this->model->freshTimestampString()],
            $values
        );

        $values[$this->qualifyColumn($column)] = $values[$column];
        if ($column != $this->qualifyColumn($column)) {
            unset($values[$column]);
        }

        return $values;
    }

}
