<?php

namespace DanielHe4rt\Scylloquent\Exceptions;

class ScylloquentSchemaException extends \Exception
{
    public static function missingPartitionKey(string $table): self
    {
        return new self('No partition/primary key has been set for the table.');
    }
}