<?php

namespace DanielHe4rt\Scylloquent\Exceptions;

class ScylloquentRuntimeException extends \RuntimeException
{
    public static function missingPartitionKey(): self
    {
        return new self('No partition/primary key has been set for the table.');
    }

    public static function notSupported(): self
    {
        return new self('ScyllaDB is not supported.');
    }
}