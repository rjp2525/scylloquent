<?php

namespace DanielHe4rt\Scylloquent\Schema;

class WithOption
{

    /** @var array<int, string> $orders */
    protected array $orders = [];

    /** @var array<int, string> $orders */
    protected array $attributes = [];

    /**
     * Add Order By Field with Direction
     */
    public function orderBy(string $field, string $dir = 'asc'): void
    {
        $this->orders[] = "\"$field\" $dir";
    }

    /**
     * Add attribute
     */
    public function attribute(string $key, string $value): void
    {
        $this->attributes[] = "$key=$value";
    }

    /**
     * Compile to CQL
     */
    public function compile(): string
    {
        if (empty($this->attributes) && empty($this->orders)) {
            return '';
        }

        $cql = 'with ';

        if (!empty($this->orders)) {
            $cql .= sprintf('clustering order by (%s) ', implode(',', $this->orders));
        }

        if (!empty($this->attributes)) {
            $cql .= implode(' AND ', $this->attributes);
        }

        return $cql;
    }
}