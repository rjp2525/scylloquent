<?php

namespace DanielHe4rt\Scylloquent\Exceptions;

use League\CommonMark\Extension\Table\Table;

class ScylloquentRuntimeException extends \RuntimeException
{
    public static function missingPartitionKey(Table $table): self
    {
        return new self('No partition/primary key has been set for the table.');
    }

    public static function notSupported(): self
    {
        return new self('ScyllaDB is not supported.');
    }
}