<?php

namespace DanielHe4rt\Scylloquent\Tests;

use DanielHe4rt\Scylloquent\ScylloquentServiceProvider;
use DanielHe4rt\Scylloquent\Tests\Traits\SetupTrait;
use function Orchestra\Testbench\artisan;

class TestCase extends \Orchestra\Testbench\TestCase
{
    use SetupTrait;

    protected function getPackageProviders($app): array
    {
        return [
            ScylloquentServiceProvider::class,
        ];
    }

    public function setUp(): void
    {
        parent::setUp();
    }

    protected function getEnvironmentSetUp($app): void
    {
        $config = require(__DIR__ . '/config/database.php');
        $this->migrateKeyspace(<<<CQL
            CREATE KEYSPACE scylloquent WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
            CQL
        );

        $app['config']->set('app.key', 'gi0BMtzVEdluo98rjx9aiFWjYtETsj8V');

        $app['config']->set('database.default', 'cassandra');
        $app['config']->set('database.connections.cassandra', $config['connections']['cassandra']);
    }

    protected function defineDatabaseMigrations()
    {
        artisan($this, 'migrate', [
            '--database' => 'cassandra',
            '--realpath' => __DIR__ . '/database',
            '--path' => [__DIR__ . '/database']
        ]);

        $this->beforeApplicationDestroyed(
            fn () => artisan($this, 'migrate:reset', [
                '--database' => 'cassandra',
                '--realpath' => __DIR__ . '/database',
                '--path' => [__DIR__ . '/database']
            ])
        );
    }
}
