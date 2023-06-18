<?php

namespace DanielHe4rt\Scylloquent;

use DanielHe4rt\Scylloquent\Repository\DatabaseMigrationRepository;
use Illuminate\Support\ServiceProvider;

class ScylloquentServiceProvider extends ServiceProvider
{
    /**
     * Register the service provider.
     */
    public function register()
    {
        // Add database driver.
        $this->app->resolving('db', function ($db) {
            $db->extend('scylla', function ($config, $name) {
                $config['name'] = $name;

                return new Connection((new ScylloquentConnector)->connect($config), $config);
            });
        });

    }

    /**
     * Boot the service provider
     */
    public function boot()
    {
        $this->app->extend('migration.repository', function ($repository, $app) {
            $table = $app['config']['database.migrations'];
            // Only in Scylla Related migrations -> rebuild commands
            return new DatabaseMigrationRepository($app['db'], $table);
        });
    }
}
