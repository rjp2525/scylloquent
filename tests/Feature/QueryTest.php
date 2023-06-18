<?php

namespace DanielHe4rt\Scylloquent\Tests\Feature;

use DanielHe4rt\Scylloquent\Tests\TestCase;
use Illuminate\Support\Facades\DB;

class QueryTest extends TestCase
{

    public function test_can_do_something()
    {
        DB::table('streamers')->get();
    }
}