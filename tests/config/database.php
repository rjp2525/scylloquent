<?php

return [

    'connections' => [

        'cassandra' => [
            'name' => 'cassandra',
            'driver' => 'cassandra',
            'host' => 'node-0.aws_us_east_1.7207ca5a8fdc45f2b03f.clusters.scylla.cloud',
            'keyspace' => 'unittest',
            'port' => 9042,
            'username' => 'scylla',
            'password' => '123',
            'consistency' => Cassandra::CONSISTENCY_LOCAL_ONE,
            'timeout' => 5.0,
            'connect_timeout' => 5.0,
            'request_timeout' => 12.0,
        ],
    ],

];
