<?php

namespace DanielHe4rt\Scylloquent\Tests\Traits;

use Symfony\Component\Process\Exception\ProcessFailedException;
use Symfony\Component\Process\Process;

trait SetupTrait
{

    protected function migrateKeyspace(
        string  $schema,
        ?array  $hosts = [],
        ?int    $port = null,
        ?string $username = null,
        ?string $password = null,
    ): void
    {
        $envHosts = env('SCYLLADB_HOSTS', $hosts);

        if (is_string($envHosts)) {
            $hosts = explode(',', $envHosts);
        }

        if (empty($hosts)) {
            $hosts = ['127.0.0.1'];
        }

        $process = new Process([
            env('CQLSH_BINARY', 'cqlsh'),
            '-u',
            env('SCYLLADB_USERNAME', $username ?? 'cassandra'),
            '-p',
            env('SCYLLADB_PASSWORD', $password ?? 'cassandra'),
            '--execute',
            $schema,
            $hosts[0],
            (int)env('SCYLLADB_PORT', $port ?? 9042),
        ]);

        $process->run();

        if (!$process->isSuccessful()) {
//         throw new ProcessFailedException($process);
        }
    }

    protected function dropKeyspace(
        string  $keyspace,
        ?array  $hosts = [],
        ?int    $port = null,
        ?string $username = null,
        ?string $password = null,
    ): void
    {
        $envHosts = env('SCYLLADB_HOSTS', $hosts);

        if (is_string($envHosts)) {
            $hosts = explode(',', $envHosts);
        }

        if (empty($hosts)) {
            $hosts = ['127.0.0.1'];
        }

        $process = new Process([
            'cqlsh',
            '-u',
            env('SCYLLADB_USERNAME', $username ?? 'scylla'),
            '-p',
            env('SCYLLADB_PASSWORD', $password ?? 'scylla'),
            '--execute',
            'DROP KEYSPACE IF EXISTS ' . $keyspace,
            $hosts[0],
            (int)env('SCYLLADB_PORT', $port ?? 9042),
        ]);

        $process->run();

        if (!$process->isSuccessful()) {
            throw new ProcessFailedException($process);
        }
    }

}
