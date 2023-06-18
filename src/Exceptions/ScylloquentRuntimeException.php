<?php

namespace DanielHe4rt\Scylloquent\Exceptions;

use League\CommonMark\Extension\Table\Table;

class ScylloquentRuntimeException extends \RuntimeException
{
    public static function notSupported(): self
    {
        return new self('ScyllaDB is not supported.');
    }
}