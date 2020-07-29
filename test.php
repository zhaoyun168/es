<?php
/**
 * Es
 */

require __DIR__.'/vendor/autoload.php';
require __DIR__.'/EsTrait.php';

use Monolog\Logger;
use Monolog\Handler\StreamHandler;

class EsConnect
{
	use Es\Common\EsTrait;

	/**
     * config
     * @var array
     */
    private $config;

	/**
     * logger
     * @var object
     */
    private $logger;

    /**
     * ES
     * @var object
     */
    private $esClient;

    /** @var array */
    private $esIndex = [];

    /** @var array 各个字符类型对应的mapping */
    private $indexMapping = [
        'integer' => ['type' => 'integer'],
        'string' => ['type' => 'string'],
        'long' => ['type' => 'long'],
        'byte' => ['type' => 'byte'],
        'date' => ['type' => 'date', 'format' => 'yyyy-MM-dd HH:mm:ss||yyyy/MM/dd HH:mm:ss||yyyy-MM-dd||yyyy/MM/dd'],
        'object' => ['type' => 'object'],
    ];

    /** @var array 各个数据表index索引信息 */
    private $tableIndex = [];

	public function __construct()
	{
		$this->config = require __DIR__.'/config.php';
        $this->config = $this->config['es'];
        $this->esClient = $this->connectElasticsearch();

        $this->esIndex = [
            'tk_tickets' => [
                'index' => 'test_tickets',
                'type' => 'tk_tickets',
            ],
        ];

        // 初始化工单索引信息
        $this->tableIndex = [
            'tk_tickets' => [
                '_source' => ['enabled' => true],
                'properties' => [
                    'id' => $this->indexMapping['long'],
                    'vcc_id' => $this->indexMapping['integer'],
                    'user_name' => $this->indexMapping['string'],
                ],
            ],
        ];

		$this->logger = new Logger('es');
        $this->logger->pushHandler(new StreamHandler('/var/log/es_test/es.log'));
	}

	/**
	 * Get Data
	 * @return array info
	 */
	public function getData()
	{
		$esClient = $this->connectElasticsearch();

		$params = [
			'index' => 'tickets',
			'type' => 'tk_tickets',
			'body' => [
				'query' => [
					'bool' => [
						'must' => [
							[
								'match' => [
									'vcc_id' => '65'
								]
							],
							[
								'wildcard' => [
									'client_name' => '*8006*'
								]
							],
							[
								'range' => [
									'create_time' => [
										'gte' => 1525104000
									]
								]
							],
							[
								'range' => [
									'create_time' => [
										'lte' => 1532534399
									]
								]
							]
						]
					]
				],
				'sort' => [
				[
					/*'create_time' => [
						'order' => 'asc',
						'unmapped_type' => 'date'
					]*/
					'ticket_priority_order' => [
						'order' => 'desc',
						'unmapped_type' => 'integer'
					]
				]
			]
			],
			'size' => 10,
			'from' => 0,
		];

		$result = $esClient->search($params);

		$sources = empty($result['hits']['hits']) ? array() : $result['hits']['hits'];//获取资源数据

		$sources_data = [];
		foreach ($sources as $key => $value) {
			$sources_data[] = $value['_source'];
		}
        $total = empty($result['hits']['total']) ? 0 : $result['hits']['total'];

        $resultData = [
        	'total' => $total,
        	'data' => $sources_data
        ];

		return $resultData;
	}

	public function setIndex(){
        $params = [
            'index' => 'test_index'
        ];

        $this->esClient->indices()->create($params);
    }
}

$esClient = new EsConnect();

//print_r($esClient->getData());

$esClient->setIndex();
