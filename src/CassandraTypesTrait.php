<?php

namespace DanielHe4rt\Scylloquent;

use Cassandra\Value;
use Illuminate\Support\Facades\Log;

trait CassandraTypesTrait
{
    /**
     * Check if object is instance of any cassandra object types
     *
     * @param $obj
     * @return bool
     */
    public function isCassandraValueObject($obj): bool
    {
        return $obj instanceof Value;
    }

    /**
     * Returns comparable value from cassandra object type
     *
     * @param $obj
     * @return mixed
     */
    public function valueFromCassandraObject($obj)
    {
        if (is_array($obj)) {
            return array_map(function ($item) {
                return $this->valueFromCassandraObject($item);
            }, $obj);
        }

        if (!is_object($obj)) {
            return $obj;
        }

        $class = get_class($obj);

        $value = match ($class) {
            'Cassandra\Date' => $obj->seconds(),
            'Cassandra\Time' => $obj->__toString(),
            'Cassandra\Timestamp' => $obj->time(),
            'Cassandra\Float' => $obj->value(),
            'Cassandra\Decimal' => $obj->value(),
            'Cassandra\Inet' => $obj->address(),
            'Cassandra\Uuid' => $obj->uuid(),
            'Cassandra\Bigint' => $obj->value(),
            'Cassandra\Blob' => $obj->toBinaryString(),
            'Cassandra\Smallint' => $obj->value(),
            'Cassandra\Timeuuid' => $obj->uuid(),
            'Cassandra\Tinyint' => $obj->value(),
            'Cassandra\Varint' => $obj->value(),
            'Cassandra\Collection', 'Cassandra\Set', 'Cassandra\Tuple' => array_map(fn($item) => $this->valueFromCassandraObject($item), $obj->values()),
            'Cassandra\UserTypeValue' => $this->valueFromCassandraObject($obj->values()),
            // 'Cassandra\Duration'
            'Cassandra\Map' => $this->valueFromCassandraMap($obj),
            default => throw ScylloquentException::typeNotDefined($class)
        };

        //TODO: convert to \DateInterval
//            case 'Cassandra\Duration':
//                $value = $obj->nanos();
//                break;

        return $value;
    }

    private function valueFromCassandraMap(mixed $obj): array
    {
        $values = array_map(function ($item) {
            return $this->valueFromCassandraObject($item);
        }, $obj->values());

        return array_combine($obj->keys(), $values);
    }

}