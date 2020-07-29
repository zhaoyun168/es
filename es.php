<?php
require __DIR__.'/vendor/autoload.php';
require __DIR__.'/esCommon.php';

use Es\Common\EsCommon;
$esClient = new EsCommon();

//创建索引
/*$result = $esClient->createIndex('ticket_test');
var_dump($result);*/

//判断索引是否存在
//echo $esClient->existsIndex('ticket_test');

//设置映射
/*$indexMapping = [
    'integer' => ['type' => 'integer'],
    'string' => ['type' => 'text',"analyzer" => "ik_max_word"], //"search_analyzer" => "ik_max_word"
    'long' => ['type' => 'long'],
    'byte' => ['type' => 'byte'],
    'date' => ['type' => 'date', 'format' => 'yyyy-MM-dd HH:mm:ss||yyyy/MM/dd HH:mm:ss||yyyy-MM-dd||yyyy/MM/dd'],
    'object' => ['type' => 'object'],
];
$mappings = [
    'tickets' => [
        '_source' => ['enabled' => true],
        'properties' => [
            'id' => $indexMapping['long'],
            'vcc_id' => $indexMapping['integer'],
            'create_time' => $indexMapping['integer'],
            'create_user_name' => $indexMapping['string'],
            'log_content' => $indexMapping['string'],
        ],
    ],
];
$result =  $esClient->setMappings('es_test', 'test', $mappings['tickets']);
var_dump($result);*/

//获取映射 es 7.X 之后去除type
/*$result = $esClient->getMappings('ticket_test', 'tickets');
print_r($result);*/

//插入单条数据
/*$item = [
    'id' => 7,
    'vcc_id' => 1,
    'create_time' => 1552889965,
    'create_user_name' => 'yuzhou1',
    'log_content' => '获取映射我爱你呀',
];
$result = $esClient->insertOne('es_test', 'test', 7, $item);

print_r($result);*/

//批量插入
/*$body = [
    [
        'index' => [
            '_index' => 'ticket_test',
        ]
    ],
    [
        'id' => 5,
        'vcc_id' => 1,
        'create_time' => 1552889961,
        'create_user_name' => 'yuzhou1aaa',
        'log_content' => '123abc123aaa'
    ],
    [
        'index' => [
            '_index' => 'ticket_test',
            '_type' => 'tickets'
        ]
    ],
    [
        'id' => 6,
        'vcc_id' => 1,
        'create_time' => 1552889961,
        'create_user_name' => 'yuzhou1bbb',
        'log_content' => '123abc123bbb'
    ]
];
$result = $esClient->bulk($body);

print_r($result);*/

//更新、没有则插入
/*$update = [
    'id' => 8,
    'vcc_id' => 1,
    'create_time' => 1552889961,
    'create_user_name' => 'yuzhou1bbbaaa',
    'log_content' => '123abc123bbbaaa'
];
$result = $esClient->updateById('ticket_test', 'tickets', 8, $update);*/

//删除单条数据
/*$result = $esClient->deleteById('ticket_test', 'tickets', 8);
print_r($result);*/

//获取分页数据
/*$result = $esClient->getDataFromPage('ticket_test', 'tickets', [], ['page' => 1, 'rows' => 3], [], false);

print_r($result);*/

//搜索数据

$items = [
    [
        'type' => 'match', 
        'field' => 'vcc_id', 
        'value' => 1
    ],
    [
        'type' => 'range', 
        'field' => 'create_time', 
        'value' => 1552889961, 
        'expression' => 'gte'
    ],
    [
        'type' => 'range', 
        'field' => 'create_time', 
        'value' => 1593330572, 
        'expression' => 'lte'
    ],
    [
        'type' => 'match', 
        'field' => 'log_content', 
        'value' => '我爱中国'
    ]
];

//$query = $esClient->processEsCondition($items);

$result = $esClient->getDataFromPage('es_test', 'test', $items, ['page' => 1, 'rows' => 20], [], false);

print_r($result);










