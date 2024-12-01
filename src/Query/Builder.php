<?php

namespace DanielHe4rt\Scylloquent\Query;

use Cassandra\Rows;
use DanielHe4rt\Scylloquent\Collection;
use DanielHe4rt\Scylloquent\Connection;
use DanielHe4rt\Scylloquent\ScyllaTypesTrait;
use Illuminate\Contracts\Database\Query\Expression as ExpressionContract;
use Illuminate\Database\Query\Builder as BaseBuilder;
use Illuminate\Support\Arr;

class Builder extends BaseBuilder
{
    use ScyllaTypesTrait;

    /**
     * Use cassandra filtering
     *
     * @var bool
     */
    public $allowFiltering = false;

    /**
     * Size of fetched page
     *
     * @var null|int
     */
    protected $pageSize = null;

    /**
     * Paginate for page
     *
     * @var null|int
     */
    protected $paginateForPage = null;

    /**
     * Pagination state token
     *
     * @var null|string
     */
    protected $paginationStateToken = null;

    /**
     * Use cassandra ttl
     *
     * @var null|int
     */
    protected $ttl = null;

    /**
     * @inheritdoc
     */
    public function __construct(Connection $connection, ?Grammar $grammar = null, ?Processor $processor = null)
    {
        $this->connection = $connection;
        $this->grammar = $grammar ?: $connection->getQueryGrammar();
        $this->processor = $processor ?: $connection->getPostProcessor();
    }

    /**
     * Support "allow filtering"
     */
    public function allowFiltering($bool = true) {
        $this->allowFiltering = (bool) $bool;

        return $this;
    }

    /**
     * Support cassandra ttl
     *
     * @param int $ttl
     *
     * @return self
     */
    public function ttl($ttl)
    {
        $this->ttl = (int) $ttl;

        return $this;
    }

    /**
     * Insert a new record into the database.
     *
     * @param  array  $values
     * @return bool
     */
    public function insert(array $values)
    {
        // Since every insert gets treated like a batch insert, we will make sure the
        // bindings are structured in a way that is convenient when building these
        // inserts statements by verifying these elements are actually an array.
        if (empty($values)) {
            return true;
        }

        if (!is_array(reset($values))) {
            $values = [$values];

            $query = empty($this->ttl)
                ? $this->grammar->compileInsert($this, $values)
                : $this->grammar->compileInsertWithOptions($this, $values, ['ttl' => $this->ttl]);

            return $this->connection->insert(
                $query,
                $this->cleanBindings(Arr::flatten($values, 1))
            );
        }

        // Here, we'll generate the insert queries for every record and send those
        // for a batch query
        else {
            $queries = [];
            $bindings = [];

            foreach ($values as $key => $value) {
                ksort($value);

                $queries[] = empty($this->ttl)
                    ? $this->grammar->compileInsert($this, $values)
                    : $this->grammar->compileInsertWithOptions($this, $values, ['ttl' => $this->ttl]);

                $bindings[] = $this->cleanBindings(Arr::flatten($value, 1));
            }

            return $this->connection->insertBulk($queries, $bindings);
        }
    }

    /**
     * Update records in the database.
     *
     * @param  array  $values
     * @return int
     */
    public function update(array $values)
    {
        $this->applyBeforeQueryCallbacks();

        $sql = empty($this->ttl)
            ? $this->grammar->compileUpdate($this, $values)
            : $this->grammar->compileUpdateWithOptions($this, $values, ['ttl' => $this->ttl]);

        return $this->connection->update($sql, $this->cleanBindings(
            $this->grammar->prepareBindingsForUpdate($this->bindings, $values)
        ));
    }

    /**
     * Execute the query as a "select" statement.
     *
     * @param  array  $columns
     *
     * @return \Illuminate\Support\Collection
     */
    public function get($columns = ['*'])
    {
        $original = $this->columns;

        if (is_null($original)) {
            $this->columns = $columns;
        }

        //Set up custom options
        $options = [];
        if ($this->pageSize !== null && (int) $this->pageSize > 0) {
            $options['page_size'] = (int) $this->pageSize;
        }
        if ($this->paginationStateToken !== null) {
            $options['paging_state_token'] = $this->paginationStateToken;
        }

        // Process select with custom options
        /** @var \Cassandra\Rows $results */
        $results = $this->processor->processSelect($this, $this->runSelect($options));

        // Make a new collection
        $collection = new Collection();

        if ($this->paginateForPage === null) {
            $this->storeInCollection($collection, $results);

            while (!$results->isLastPage()) {
                $results = $results->nextPage();
                foreach ($results as $row) {
                    $collection->push($row);
                }
            }
        } else {

            $loopingPage = 0;
            while (true) {
                $loopingPage++;
                if ($loopingPage !== $this->paginateForPage) {
                    if ($results->isLastPage()) {
                        break;
                    }

                    $results = $results->nextPage(2);
                    continue;
                }

                foreach ($results as $row) {
                    $this->storeInCollection($collection, $results);
                }
            }
        }

        $collection->setRowsInstance($results);

        $this->columns = $original;

        return $collection;
    }

    /**
     * Currently we are doing cursor based pagination
     * TODO
     * 1. Implementing mechanism for jumping into a page
     * 2. Taking columns in consideration
     *
     * @param integer $perPage
     *
     * @return Collection
     */
    public function paginate($perPage = 15, $columns = [...], $pageName = 'page', $page = null, $total = null)
    {
        $option = ['page_size' => $perPage];

        if (!empty($this->paginationStateToken)) {
            $option['paging_state_token'] = $this->paginationStateToken;
        }

        return $this->runSelect($option);
    }

    /**
     * Run the query as a "select" statement against the connection.
     *
     * @param array $options
     *
     * @return array
     */
    protected function runSelect(array $options = [])
    {
        return $this->connection->select(
            $this->toSql(), $this->getBindings(), !$this->useWritePdo, $options
        );
    }

    /**
     * Set pagination state token to fetch
     * next page
     *
     * @param string $token
     *
     * @return Builder
     */
    public function setPaginationStateToken($token = null)
    {
        $this->paginationStateToken = $token;

        return $this;
    }

    /**
     * Set page size
     *
     * @param int $pageSize
     *
     * @return Builder
     */
    public function setPageSize($pageSize = null)
    {
        $this->pageSize = $pageSize !== null ? (int) $pageSize : $pageSize;

        return $this;
    }

    /**
     * Set the limit and offset for a given page.
     *
     * @param  int  $page
     * @param  int  $perPage
     * @return \Illuminate\Database\Query\Builder|static
     */
    public function forPage($page, $perPage = 15)
    {
        $this->paginateForPage = (int) $page;

        return $this->setPageSize($perPage);
    }

    /**
     * Store in Collections
     */
    protected function storeInCollection(Collection $collection, Rows $results): Collection
    {
        foreach ($results as $item) {
            $collection->push($item);
        }

        return $collection;
    }

    public function cleanBindings(array $bindings)
    {
        return collect($bindings)
            ->reject(function ($binding) {
                return $binding instanceof ExpressionContract;
            })
            //->map([$this, 'castBinding'])
            ->values()
            ->all();
    }
}
