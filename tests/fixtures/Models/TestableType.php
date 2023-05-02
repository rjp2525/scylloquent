<?php

namespace DanielHe4rt\Scylloquent\Fixtures\Models;

use DanielHe4rt\Scylloquent\Eloquent\Model;

class TestableType extends Model
{
    protected $keyType = 'uuid';

    public $incrementing = false;

    protected $casts = [
        'date' => 'date',
        'datetime' => 'datetime',
        'time' => 'string',
    ];
}