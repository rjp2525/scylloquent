<?php

namespace DanielHe4rt\Scylloquent;

use \Cassandra\Rows;
use DanielHe4rt\Scylloquent\Eloquent\Model;
use Illuminate\Database\Eloquent\Collection as BaseCollection;

class Collection extends BaseCollection
{
    /**
     * Cassandra rows instance
     *
     * @var \Cassandra\Rows
     */
    private $rows;

    /**
     * Set Cassandra rows instance related to the
     * collection items.
     *
     * Required for fetching next pages
     *
     * @param Rows $rows
     *
     * @return $this
     */
    public function setRowsInstance(Rows $rows)
    {
        $this->rows = $rows;

        return $this;
    }

    /**
     * Next page token
     *
     * @return mixed
     */
    public function getNextPageToken()
    {
        if ($this->rows === null) {
            return null;
        }

        return $this->rows->pagingStateToken();
    }

    /**
     * Last page indicator
     * @return bool
     */
    public function isLastPage()
    {
        if ($this->rows === null) {
            return true;
        }

        return $this->rows->isLastPage();
    }

    /**
     * Get next page
     *
     * @return Collection
     */
    public function nextPage(): Collection
    {
        if ($this->rows !== null && !$this->isLastPage()) {
            /** @var Model $model */
            $model = $this->first();

            $nextPageRows = $this->rows->nextPage(5);

            return $model->newCassandraCollection($nextPageRows);
        }

        return new self();
    }

    /** Get rows instance */
    public function getRows(): Rows
    {
        return $this->rows;
    }

    /**
     * Update current collection with results from the next page
     */
    public function appendNextPage(): self
    {
        $nextPage = $this->nextPage();

        if (!$nextPage->isEmpty()) {
            $this->items = array_merge($this->items, $nextPage->toArray());
            $this->rows = $nextPage->getRows();
        }

        return $this;
    }

    /**
     * Merge the collection with the given items.
     *
     * @param  \ArrayAccess|array  $items
     */
    public function merge($items): self
    {
        $dictionary = $this->getDictionary();

        foreach ($items as $item) {
            $dictionary[(string) $item->getKey()] = $item;
        }

        return new self(array_values($dictionary));
    }

    /**
     * Reload a fresh model instance from the database for all the entities.
     *
     */
    public function fresh($with = []): self
    {
        if ($this->isEmpty()) {
            return new self();
        }

        /** @var Model $model */
        $model = $this->first();

        $freshModels = $model->newQueryWithoutScopes()
            ->whereIn($model->getKeyName(), $this->modelKeys())
            ->get()
            ->getDictionary();

        return $this->map(function ($model) use ($freshModels) {
            if ($model->exists && isset($freshModels[$model->getKey()])) {
                return $freshModels[$model->getKey()];
            }

            return null;
        });
    }
}
