<?php

return [

    'connections' => [
        'cassandra' => [
            'name' => 'cassandra',
            'driver' => 'cassandra',
            'host' => 'localhost',
            'keyspace' => 'scylloquent',
            'port' => 9042,
            'username' => 'scylla',
            'password' => 'a',
            'consistency' => Cassandra::CONSISTENCY_LOCAL_ONE,
            'timeout' => 5.0,
            'connect_timeout' => 3.0,
            'request_timeout' => 3.0,
            'tls' => null,
            'scheme' => null,
        ],
    ],

];
