<?php
require 'vendor/autoload.php';

use Elasticsearch\ClientBuilder;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

$logger = new Logger('name');
$logger->pushHandler(new StreamHandler('./log/es.log', Logger::WARNING));

//$client = ClientBuilder::create()->setHosts([['host' => '192.168.100.115', 'port' => '9200', 'user' => 'elastic', 'pass' => '123456']])->setLogger($logger)->build(); //elastic
$client = ClientBuilder::create()->setHosts(['192.168.100.105:9200'])->setLogger($logger)->build(); //elastic

//索引文档
/*$params = [
    'index' => 'my_index',
    'id'    => '1',
    'body'  => ['title' => '我是中国人', 'content' => '热爱']
];

echo '<pre>';
$response = $client->index($params);
print_r($response);*/

//获取文档
/*$params = [
    'index' => 'my_index',
    'id'    => 'my_id'
];

echo '<pre>';
$response = $client->get($params);
print_r($response);*/

//搜索文档
/*$params = [
    'index' => 'my_index',
    'body'  => [
        'query' => [
            'match' => [
                'title' => '中国人'
            ]
        ]
    ]
];

echo '<pre>';
$response = $client->search($params);
print_r($response);*/

//删除文档
/*$params = [
    'index' => 'my_index',
    'id'    => 'my_id'
];

echo '<pre>';
$response = $client->delete($params);
print_r($response);*/

//删除索引
/*$deleteParams = [
    'index' => 'my_index'
];
$response = $client->indices()->delete($deleteParams);
print_r($response);*/

//创建有参数的索引
/*$params = [
    'index' => 'my_index_test_2',
    'body' => [
        'settings' => [
            'number_of_shards' => 2,
            'number_of_replicas' => 0
        ]
    ]
];

$response = $client->indices()->create($params);
print_r($response);*/

/*$params = [
    'index' => 'my_index',
    'body' => [
        'settings' => [
            'number_of_shards' => 3,
            'number_of_replicas' => 0
        ],
        'mappings' => [
            '_source' => [
                'enabled' => true
            ],
            'properties' => [
                'first_name' => [
                    'type' => 'keyword'
                ],
                'age' => [
                    'type' => 'integer'
                ]
            ]
        ]
    ]
];


// Create the index with mappings and settings now
$response = $client->indices()->create($params);*/


/*$params = [
    'index' => 'reuters',
    'body' => [
        'settings' => [ 
            'number_of_shards' => 1,
            'number_of_replicas' => 0,
            'analysis' => [ 
                'filter' => [
                    'shingle' => [
                        'type' => 'shingle'
                    ]
                ],
                'char_filter' => [
                    'pre_negs' => [
                        'type' => 'pattern_replace',
                        'pattern' => '(\\w+)\\s+((?i:never|no|nothing|nowhere|noone|none|not|havent|hasnt|hadnt|cant|couldnt|shouldnt|wont|wouldnt|dont|doesnt|didnt|isnt|arent|aint))\\b',
                        'replacement' => '~$1 $2'
                    ],
                    'post_negs' => [
                        'type' => 'pattern_replace',
                        'pattern' => '\\b((?i:never|no|nothing|nowhere|noone|none|not|havent|hasnt|hadnt|cant|couldnt|shouldnt|wont|wouldnt|dont|doesnt|didnt|isnt|arent|aint))\\s+(\\w+)',
                        'replacement' => '$1 ~$2'
                    ]
                ],
                'analyzer' => [
                    'reuters' => [
                        'type' => 'custom',
                        'tokenizer' => 'standard',
                        'filter' => ['lowercase', 'stop', 'kstem']
                    ]
                ]
            ]
        ],
        'mappings' => [ 
            'properties' => [
                'title' => [
                    'type' => 'text',
                    'analyzer' => 'reuters',
                    'copy_to' => 'combined'
                ],
                'body' => [
                    'type' => 'text',
                    'analyzer' => 'reuters',
                    'copy_to' => 'combined'
                ],
                'combined' => [
                    'type' => 'text',
                    'analyzer' => 'reuters'
                ],
                'topics' => [
                    'type' => 'keyword'
                ],
                'places' => [
                    'type' => 'keyword'
                ]
            ]
        ]
    ]
];
$client->indices()->create($params);
*/

//更新配置
/*$params = [
    'index' => 'my_index',
    'body' => [
        'settings' => [
            'number_of_replicas' => 0,
            'refresh_interval' => -1
        ]
    ]
];

$response = $client->indices()->putSettings($params);

print_r($response);*/

// Get settings for one index
/*$params = ['index' => 'my_index'];
$response = $client->indices()->getSettings($params);

print_r($response);

// Get settings for several indices
$params = [
    'index' => [ 'my_index', 'my_index2' ]
];
$response = $client->indices()->getSettings($params);

print_r($response);*/


//修改映射
// Set the index and type
/*$params = [
    'index' => 'my_index',
    'body' => [
        '_source' => [
            'enabled' => true
        ],
        'properties' => [
            'first_name' => [
                'type' => 'text',
                //'analyzer' => 'standard'
            ],
            'age' => [
                'type' => 'integer'
            ]
        ]
    ]
];

// Update the index mapping
$client->indices()->putMapping($params);*/

//批量索引文档
/*for($i = 0; $i < 100; $i++) {
    $params['body'][] = [
        'index' => [
            '_index' => 'my_index',
        ]
    ];

    $params['body'][] = [
        'first_name'     => '天宇',
        'age' => 20
    ];
}

$responses = $client->bulk($params);

print_r($responses);*/

/*$params = ['body' => []];

for ($i = 1; $i <= 10000; $i++) {
    $params['body'][] = [
        'index' => [
            '_index' => 'my_index',
            '_id'    => $i
        ]
    ];

    $params['body'][] = [
        'first_name'     => '天宇',
        'age' => 20
    ];

    // Every 1000 documents stop and send the bulk request
    if ($i % 1000 == 0) {
        $responses = $client->bulk($params);

        // erase the old bulk request
        $params = ['body' => []];

        // unset the bulk response when you are done to save memory
        unset($responses);
    }
}

// Send the last batch if it exists
if (!empty($params['body'])) {
    $responses = $client->bulk($params);
}*/

// Get mappings for all indexes
/*$response = $client->indices()->getMapping();

// Get mappings in 'my_index'
$params = ['index' => 'my_index'];
$response = $client->indices()->getMapping($params);

print_r($response);

// Get mappings for two indexes
$params = [
    'index' => [ 'my_index', 'my_index2' ]
];
$response = $client->indices()->getMapping($params);

print_r($response);*/


/*$params = [
    'index' => 'my_index',
    'id'    => '3_4SH28BKJuVPjc06XSs'
];

// Get doc at /my_index/_doc/my_id
$response = $client->get($params);

print_r($response);*/

/*$params = [
    'index' => 'my_index',
    'id'    => '3_4SH28BKJuVPjc06XSs',
    'body'  => [
        'doc' => [
            'first_name' => 'abc',
            'age' => 21,
        ]
    ]
];

// Update doc at /my_index/_doc/my_id
$response = $client->update($params);

print_r($response);*/

/*$params = [
    'index' => 'my_index',
    'id'    => '3_4SH28BKJuVPjc06XSs'
];

// Delete doc at /my_index/_doc_/my_id
$response = $client->delete($params);

print_r($response);*/

//匹配查询
/*$params = [
    'index' => 'my_index',
    'body'  => [
        'query' => [
            'match' => [
                'first_name' => '天宇'
            ]
        ]
    ]
];

$results = $client->search($params);

$data = isset($results['hits']['hits']) && !empty($results['hits']['hits']) ? $results['hits']['hits'] : [];

$result_data = [];
if (!empty($data) && is_array($data)) {
    foreach ($data as $key => $value) {
        if (isset($value['_source']) && !empty($value['_source'])) {
            $result_data[] = $value['_source'];
        }
    }
}

print_r($result_data);*/

//and 查询
/*$params = [
    'index' => 'my_index',
    'body'  => [
        'query' => [
            'bool' => [
                'must' => [
                    [ 'match' => [ 'first_name' => '天宇' ] ],
                    [ 'match' => [ 'age' => 20 ] ],
                ]
            ]
        ]
    ]
];

$results = $client->search($params);

print_r($results);*/

$params = [
    'index' => 'my_index',
    'body'  => [
        'query' => [
            'bool' => [
                'filter' => [
                    'term' => [ 'first_name' => '天宇' ]
                ],
                'should' => [
                    'match' => [ 'age' => 20 ]
                ]
            ]
        ]
    ]
];


$results = $client->search($params);

print_r($results);