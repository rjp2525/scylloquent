<?php

namespace DanielHe4rt\Scylloquent\Fixtures\Models;

use DanielHe4rt\Scylloquent\Eloquent\Model;

class Book extends Model
{
    protected static $unguarded = true;
    protected $primaryKey = 'title';
}
