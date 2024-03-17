<?php

namespace DanielHe4rt\Scylloquent\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

class CreateScyllaKeyspaceCommand extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'make:keyspace';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Create a new ScyllaDB Keyspace';

    private string $keyspaceQuery = "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'AWS_SA_EAST_1' : 3} AND durable_writes = true";

    public function handle(): int
    {
        $this->info('Creating a new ScyllaDB Keyspace');

        $keyspace = $this->ask('What is the name of the new keyspace?',
            config('database.connections.scylla.keyspace')
        );

        try {
            DB::connection('scylla')->statement(sprintf($this->keyspaceQuery, $keyspace));
            $this->info('Keyspace created successfully');

            return self::SUCCESS;
        } catch (\Throwable $th) {
            if (str_contains($th->getMessage(), 'existing keyspace')) {
                return $this->handleKeyspaceConflict($keyspace);
            } else {
                $this->warn('Error creating keyspace');
                $this->comment($th->getMessage());
            }

        }
        $this->warn('Error creating keyspace');
        return self::FAILURE;
    }

    private function handleKeyspaceConflict(string $keyspace): int
    {
        $this->warn("Keyspace $keyspace already exists");
        $keyspaceCreationStatus = $this->choice('What do you want to do?', [
            'Drop and recreate',
            'Do nothing'
        ], 0);

        if ($keyspaceCreationStatus == 1) {
            $this->info('Keyspace creation aborted');
            return self::SUCCESS;
        }

        try {
            DB::connection('scylla')->statement("DROP KEYSPACE $keyspace");
            DB::connection('scylla')->statement(sprintf($this->keyspaceQuery, $keyspace));
            $this->info('Keyspace dropped and recreated!');
            return self::SUCCESS;
        } catch (\Throwable $th) {
            $this->warn('Error dropping keyspace');
            $this->comment($th->getMessage());
        }

        return self::FAILURE;
    }
}
