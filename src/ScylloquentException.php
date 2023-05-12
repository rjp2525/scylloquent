<?php

namespace DanielHe4rt\Scylloquent;

use Symfony\Component\HttpFoundation\Response;

class ScylloquentException extends \Exception
{
    public static function typeNotDefined(string $classPath): self
    {
        return new self(
            sprintf('The type -> %s does not exists.', $classPath),
            Response::HTTP_INTERNAL_SERVER_ERROR
        );
    }
}