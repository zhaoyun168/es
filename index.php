<?php

use Elasticsearch\ClientBuilder;

require './vendor/autoload.php';

$client = connectElasticsearch();


//require 'vendor/autoload.php';    //加载自动加载文件
 
#如果没有设置主机地址默认为127.0.0.1:9200
//$client = Elasticsearch\ClientBuilder::create()->setHosts(['localhost:9200'])->build();
 
/**
* 创建库索引的mapping结构
*/
/*$params = [
	'index' => 'my_index',  //索引名（相当于mysql的数据库）
	'body' => [
		'settings' => [
			'number_of_shards' => 5,  #分片数
		],
		'mappings' => [ 
			'my_type' => [ //类型名（相当于mysql的表）
				'_all' => [
					'enabled' => 'false'
				],
				'_routing' => [
					'required' => 'true'
				],
				'properties' => [ //文档类型设置（相当于mysql的数据类型）
					'name' => [
						'type' => 'string',
						'store' => 'true'
					],
					'age' => [
						'type' => 'integer'
					]
				]
			]
		]
	]
];
 
$res = $client->indices()->create($params);   //创建库索引*/
 
/**
* 库索引操作
*/
$params = [
	'index' => 'tickets',
	'client' => [
		'ignore' => 404
	]
];
//$res = $client->indices()->delete($params);    //删除库索引
//$res = $client->indices()->getSettings($params);//获取库索引设置信息
//$res = $client->indices()->exists($params);   //检测库是否存在
$res = $client->indices()->getMapping($params);   //获取mapping信息
print_r($res);
 


//var_dump($es_conn);

/**
 * 获取ES连接
 *
 * @return \Elasticsearch\Client
 */
function connectElasticsearch()
{
    //获取elasticsearch全文检索主机
    $esHosts = array('127.0.0.1:9200');
    try {
        $clientBuilder = ClientBuilder::create();
        $clientBuilder->setHosts($esHosts);
        $clientBuilder->setRetries(50);
        //$clientBuilder->setLogger($this->logger);
        $esClient = $clientBuilder->build();
        $esClient->ping();
    } catch (\Exception $e) {
        echo sprintf(
            "连接Elasticsearch失败,失败原因为[%s].",
            $e->getMessage()
        );
    }

    return $esClient;
}


