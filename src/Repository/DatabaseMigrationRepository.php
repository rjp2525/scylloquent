<?php

namespace DanielHe4rt\Scylloquent\Repository;

use Cassandra\Uuid;
use Illuminate\Database\Migrations\DatabaseMigrationRepository as BaseDatabaseMigrationRepository;

class DatabaseMigrationRepository extends BaseDatabaseMigrationRepository
{
    /**
     * Get the completed migrations.
     *
     * @return array
     */
    public function getRan(): array
    {
        return $this->table()
            ->get()
            ->sortBy('batch')
            ->pluck('migration')
            ->all();
    }

    /**
     * Get list of migrations.
     *
     * @param  int  $steps
     */
    public function getMigrations($steps): array
    {
        return $this->table()
            ->where('batch', '>=', '1')
            ->get()
            ->sortByDesc('batch')
            ->take($steps)
            ->all();
    }

    /**
     * Get the last migration batch.
     */
    public function getLast(): array
    {
        return $this->table()
            ->where('batch', '=', $this->getLastBatchNumber())
            ->get()
            ->all();
    }

    /**
     * Create the migration repository data store.
     *
     * @return void
     */
    public function createRepository()
    {
        $schema = $this->getConnection()->getSchemaBuilder();

        $schema->create($this->table, function ($table) {
            // The migrations table is responsible for keeping track of which of the
            // migrations have actually run for the application. We'll create the
            // table to hold the migration file's path as well as the batch ID.
            $table->uuid('id');
            $table->string('migration');
            $table->integer('batch');
            $table->primary(['id', 'batch']);
        });
    }

    /**
     * Log that a migration was run.
     *
     * @param  string  $file
     * @param  int  $batch
     * @return void
     */
    public function log($file, $batch)
    {
        $record = [
            'id' => \Ramsey\Uuid\Uuid::uuid4()->toString(),
            'migration' => $file,
            'batch' => $batch
        ];

        $this->table()->insert($record);
    }

    /**
     * Remove a migration from the log.
     *
     * @param  object  $migration
     * @return void
     */
    public function delete($migration)
    {
        $this->table()
            ->where('migration', '=', $migration->migration)
            ->delete();
    }
}