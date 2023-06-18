<?php

namespace DanielHe4rt\Scylloquent\Tests\Unit\Schema;

use DanielHe4rt\Scylloquent\Schema\Blueprint;
use DanielHe4rt\Scylloquent\Tests\TestCase;

class BlueprintTest extends TestCase
{

    public function test_blueprint_can_generate_partition_key()
    {
        $blueprint = new Blueprint('fodase', fn () => true);
    }
}