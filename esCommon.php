<?php

/**
 * ES Common
 */

namespace Es\Common;

use Elasticsearch\ClientBuilder;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

/**
 * Class EsCommon.
 */
class EsCommon
{
    /** @var Logger */
    private $logger;

    /** @var array */
    private $config = [];

    /** @var \Elasticsearch\Client */
    private $client = null;

    /** @var array 索引配置 */
    private $indexConfig = [
        'number_of_shards' => 6,
        'number_of_replicas' => 1,
        'refresh_interval' => '5s',
        'index.mapping.total_fields.limit' => 10000000,
    ];

    /**
     * EsCommon constructor.
     *
     * @param Logger             $logger
     * @param array              $config
     *
     * @throws \Exception
     */
    public function __construct()
    {
        $this->logger = new Logger('es');
        $this->logger->pushHandler(new StreamHandler('/var/log/es_test/es.log'));
        $this->config = require __DIR__.'/config.php';
        $this->client = $this->connect();
    }

    /**
     * 连接Elasticsearch.
     *
     * @return \Elasticsearch\Client
     *
     * @throws \Exception
     */
    public function connect()
    {
        $client = null;
        $esHost = empty($this->config['es']['host']) ? '' : $this->config['es']['host'];
        if (empty($esHost)) {
            throw new \Exception('elasticsearch连接地址未配置');
        }
        $connectTimes = 0;
        connect_elastic:
        try {
            $clientBuilder = ClientBuilder::create();
            $clientBuilder->setHosts($esHost);
            $clientBuilder->setRetries(2);
            $clientBuilder->setLogger($this->logger);
            $client = $clientBuilder->build();
            if ($client->ping()) {
                return $client;
            }
        } catch (\Exception $e) {
            ++$connectTimes;
            $this->logger->error(sprintf(
                '第【%s】次连接elasticsearch异常, 异常信息为【%s】',
                $connectTimes,
                $e->getMessage()
            ));
            sleep(2);
            if ($connectTimes < 3) {
                goto connect_elastic;
            }
        }

        throw new \Exception(sprintf('ES集群【%s】连接不上', $esHost));
    }

    /**
     * 创建索引.
     *
     * @param string $index
     * @param array  $option
     *
     * @return array
     */
    public function createIndex($index, $option = [])
    {
        try {
            $exist = $this->client->indices()->exists(['index' => $index]);
            if ($exist) {
                return ['code' => 0, 'message' => 'ok'];
            }
            $settings = empty($option) ? $this->indexConfig : $option;
            $params = [
                'index' => $index,
                'body' => [
                    'settings' => $settings,
                ],
            ];
            $this->client->indices()->create($params);

            // 创建成功
            return ['code' => 0, 'message' => 'ok'];
        } catch (\Exception $e) {
            $this->logger->error(sprintf('创建索引【%s】异常, 异常信息为【%s】', $index, $e->getMessage()));

            return ['code' => 400, 'message' => '索引创建失败'];
        }
    }

    /**
     * 判断索引是否存在.
     *
     * @param string $index
     *
     * @return bool
     */
    public function existsIndex($index)
    {
        $exist = false;
        try {
            $exist = $this->client->indices()->exists(['index' => $index]);
        } catch (\Exception $e) {
            $this->logger->error(sprintf('elastic判断索引【%s】异常, 异常信息为【%s】', $index, $e->getMessage()));
        }

        return $exist;
    }

    /**
     * 设置Mapping.
     *
     * @param string $index
     * @param string $type
     * @param array  $mappings
     *
     * @return array
     */
    public function setMappings($index, $type, $mappings = [])
    {
        try {
            $params = [
                'index' => $index,
                'include_type_name' => true,
                'type' => $type,
                'body' => [
                    $type => $mappings,
                ],
            ];
            $this->client->indices()->putMapping($params);

            return ['code' => 0, 'message' => 'ok'];
        } catch (\Exception $e) {
            $this->logger->error(sprintf('【%s-%s】设置mapping异常, 异常信息为【%s】', $index, $type, $e->getMessage()));

            return ['code' => 400, 'message' => '设置mapping失败'];
        }
    }

    /**
     * 获取Mapping.
     *
     * @param string $index
     * @param string $type
     *
     * @return array
     */
    public function getMappings($index, $type)
    {
        try {
            $params = [
                'index' => $index,
                //'type' => $type,
            ];
            $response = $this->client->indices()->getMapping($params);

            return ['code' => 0, 'message' => 'ok', 'data' => $response];
        } catch (\Exception $e) {
            $this->logger->error(sprintf('获取【%s-%s】的mapping异常, 异常信息为【%s】', $index, $type, $e->getMessage()));

            return ['code' => 400, 'message' => '获取mapping失败'];
        }
    }

    /**
     * 插入单条数据.
     *
     * @param string $index
     * @param string $type
     * @param string $id
     * @param array  $item
     * @param bool   $refresh
     *
     * @return array
     */
    public function insertOne($index, $type, $id, $item, $refresh = true)
    {
        $params = [
            'index' => $index,
            //'type' => $type,
            'id' => $id,
            'body' => $item,
            'refresh' => $refresh,
        ];
        try {
            $response = $this->client->index($params);

            return ['code' => 0, 'message' => 'ok', 'data' => $response['_id']];
        } catch (\Exception $e) {
            $this->logger->error(sprintf('数据【%s】索引异常, 异常信息为【%s】', json_encode($params), $e->getMessage()));

            return ['code' => 400, 'message' => '数据索引失败'];
        }
    }

    /**
     * 插入、更新多条数据.
     *
     * @param array $body
     * @param bool  $refresh
     *
     * @return array|bool 返回失败的id数组
     */
    public function bulk($body, $refresh = true)
    {
        try {
            $params = ['body' => $body, 'refresh' => $refresh];
            $response = $this->client->bulk($params);
            $errors = empty($response['errors']) ? 0 : $response['errors'];
            $errorRes = [];
            $items = empty($response['items']) ? [] : $response['items'];
            foreach ($items as $item) {
                foreach ($item as $key => $value) {
                    if (isset($value['error'])) {
                        //bulk错误的数据
                        $errorRes[] = $value;
                        continue;
                    }
                }
            }
            if ($errors > 0) {
                $this->logger->warning(sprintf(
                    '发送数据【%s】到Elasticsearch失败, 失败原因为【%s】',
                    json_encode($body),
                    json_encode($errorRes)
                ));
            }

            return ['code' => 0, 'message' => 'ok'];
        } catch (\Exception $e) {
            $this->logger->error(sprintf(
                'bulk操作失败, 操作数据为【%s】, 失败原因为【%s】',
                json_encode($params),
                $e->getMessage()
            ));

            return ['code' => 400, 'message' => 'bulk操作失败'];
        }
    }

    /**
     * 更新单条数据（if not exist insert）.
     *
     * @param string $index
     * @param string $type
     * @param string $id
     * @param array  $update
     * @param bool   $refresh 刷新ES
     *
     * @return array
     */
    public function updateById($index, $type, $id, $update,$refresh = true)
    {
        $params = [
            'index' => $index,
            'type' => $type,
            'id' => $id,
            'retry_on_conflict' => 3,
            'body' => ['doc' => $update, 'doc_as_upsert' => true],
            'refresh' => $refresh,
        ];
        try {
            $this->client->update($params);

            return ['code' => 0, 'message' => 'ok'];
        } catch (\Exception $e) {
            $this->logger->error(sprintf('数据【%s】索引更新失败, 失败原因为【%s】', json_encode($params), $e->getMessage()));

            return ['code' => 400, 'message' => '数据索引更新失败'];
        }
    }

    /**
     * 删除单条数据.
     *
     * @param string $index
     * @param string $type
     * @param string $id
     * @param bool   $refresh
     *
     * @return array
     */
    public function deleteById($index, $type, $id, $refresh = true)
    {
        $params = [
            'index' => $index,
            'type' => $type,
            'id' => $id,
            'refresh' => $refresh,
        ];
        try {
            $this->client->delete($params);

            return ['code' => 0, 'message' => 'ok'];
        } catch (\Exception $e) {
            $this->logger->error(sprintf('数据【%s】索引删除失败, 失败原因为【%s】', json_encode($params), $e->getMessage()));

            return ['code' => 400, 'message' => '删除索引数据失败'];
        }
    }

    /**
     * 获取分页数据.
     *
     * @param string $index
     * @param string $type
     * @param array  $query
     * @param array  $pageInfo
     * @param array  $sortInfo
     * @param bool   $returnFlag
     *
     * @return array
     */
    public function getDataFromPage($index, $type, $query, $pageInfo, $sortInfo = [], $returnFlag = true)
    {
        $total = 0;
        $items = [];
        try {
            // 分页
            $page = empty($pageInfo['page']) ? 1 : $pageInfo['page'];
            $rows = empty($pageInfo['rows']) ? 10 : $pageInfo['rows'];
            // 获取真实页码数
            $total = $this->getCount($index, $type, $query);
            $totalPage = ceil($total / $rows);
            $page = $page > $totalPage ? $totalPage : $page;
            $from = ($page - 1) * $rows;
            $from = $from > 0 ? $from : 0;
            $body = [];
            // 排序
            if (!empty($sortInfo) && is_array($sortInfo)) {
                foreach ($sortInfo as $ksort => $vsort) {
                    if (!empty($vsort['sort'])) {
                        $order = empty($vsort['order']) ? 'desc' : $vsort['order'];
                        $body['sort'][] = [$vsort['sort'] => ['order' => $order, 'unmapped_type' => 'date']];
                    }
                }
            }
            $this->logger->info(sprintf('Es排序为【%s】', empty($body['sort']) ? '空' : json_encode($body['sort'])));
            // 条件
            if (!empty($query)) {
                $this->logger->info(sprintf('Es原始查询条件为【%s】', json_encode($query)));
                $body['query'] = $this->processEsCondition($query);
                $this->logger->info(sprintf('Es查询条件为【%s】', json_encode($body['query'])));
            }

            // 只取id
            if ($returnFlag) {
                $body['_source'] = ['includes' => ['id']];
            }

            // 大于10000时进行滚动获取数据
            if ($from + $rows > 10000) {
                $body['sort']['_doc'] = ['order' => 'asc'];
                $params = [
                    'index' => $index,
                    //'type' => $type,
                    'body' => $body,
                    'from' => 0,
                    'size' => 500,
                    'scroll' => '5s',
                ];
                $res = $this->client->search($params);
                $size = $rows;
                $index = 0;
                while (isset($res['hits']['hits']) && count($res['hits']['hits']) > 0) {
                    // 获取scrollid
                    $scrollId = $res['_scroll_id'];
                    if ($from >= $index && $from < $index + 500) {
                        for ($i = $from - $index; $i < count($res['hits']['hits']); ++$i) {
                            if ($returnFlag) {
                                $items[] = (int) $res['hits']['hits'][$i]['_source']['id'];
                            } else {
                                $item = $res['hits']['hits'][$i]['_source'];
                                $items[] = $item;
                            }
                            ++$from;
                            --$size;
                            if (0 === $size) {
                                return ['total' => $total, 'rows' => $items];
                            }
                        }
                    }
                    $index = $index + 500;
                    $res = $this->client->scroll([
                        'scroll_id' => $scrollId,
                        'scroll' => '5s',
                    ]);
                }
                $this->client->clearScroll($res['_scroll_id']);
            } else {
                $params = [
                    'index' => $index,
                    //'type' => $type,
                    'body' => $body,
                    'from' => $from,
                    'size' => $rows,
                ];
                $res = $this->client->search($params);
                foreach ($res['hits']['hits'] as $value) {
                    if ($returnFlag) {
                        $items[] = empty($value['_source']['id']) ? 0 : (int) $value['_source']['id'];
                        continue;
                    }
                    $item = $value['_source'];
                    $items[] = $item;
                }
            }
        } catch (\Exception $e) {
            $this->logger->error(sprintf('获取elastic分页数据异常, 异常信息为【%s】', $e->getMessage()));
        }

        return ['total' => $total, 'rows' => $items];
    }

    /**
     * 查询数据总条数.
     *
     * @param string $index
     * @param string $type
     * @param array  $query
     *
     * @return int
     */
    public function getCount($index, $type, $query = [])
    {
        $count = 0;
        $params = [
            'index' => $index,
            //'type' => $type,
        ];
        if (!empty($query)) {
            $query = $this->processEsCondition($query);
            $params['body']['query'] = $query;
        }
        try {
            $res = $this->client->count($params);
            $count = empty($res['count']) ? 0 : $res['count'];
        } catch (\Exception $e) {
            $this->logger->error(sprintf('elastic查询数据总条数异常, 异常信息为【%s】', $e->getMessage()));
        }

        return $count;
    }

    /**
     * 数据聚合.
     *
     * @param string $index
     * @param string $type
     * @param array  $aggs
     * @param array  $query
     *
     * @return array
     */
    public function getAggs($index, $type, $aggs, $query = [])
    {
        $result = [];
        $params = [
            'index' => $index,
            'type' => $type,
            'body' => ['aggs' => $aggs],
        ];
        if (!empty($query)) {
            $query = $this->processEsCondition($query);
            $params['body']['query'] = $query;
        }
        try {
            $result = $this->client->search($params);
        } catch (\Exception $e) {
            $this->logger->error(sprintf('elastic数据聚合【%s】异常, 异常信息为【%s】', json_encode($aggs), $e->getMessage()));
        }

        return $result;
    }

    /**
     * 处理ES查询条件.
     *
     * @param array $items 查询条目 array(
     *                     array(
     *                     "type" => "match",
     *                     "field" => "ag_num",
     *                     "value" => "1001"
     *                     ), array(
     *                     "type" => "terms",
     *                     "field" => "ag_name",
     *                     "value" => "王"
     *                     ),array(
     *                     "type" => "range",
     *                     "field" => "start_time",
     *                     "expression" => "gte",
     *                     "value" => "1489991897"
     *                     ), array(
     *                     "type" => "range",
     *                     "field" => "start_time",
     *                     "expression" => "lte",
     *                     "value" => "1489991900"
     *                     ), array(
     *                     "type" => "should",
     *                     "field" => "ag_status",
     *                     "value" => "1,3,4"
     *                     )
     *                     )
     *
     * @return array
     */
    public function processEsCondition($items)
    {
        $query = [];
        if (!empty($items)) {
            foreach ($items as $key => $item) {
                switch ($item['type']) {
                    case 'match':
                        //精准匹配
                        if (!empty($item['field']) && (!empty($item['value']) || 0 === $item['value'] || '' === $item['value'])) {
                            $query['bool']['must'][] = ['match' => [$item['field'] => $item['value']]];
                        }
                        break;
                    case 'term':
                        //聚合匹配
                        if (!empty($item['field']) && (!empty($item['value'] || 0 === $item['value']))) {
                            $query['bool']['must'][] = ['term' => [$item['field'] => $item['value']]];
                        }
                        break;
                    case 'multi_must_term':
                        $values = explode(',', $item['value']);
                        if (!empty($values) && is_array($values)) {
                            foreach ($values as $val) {
                                $query['bool']['must'][] = ['term' => [$item['field'] => $val]];
                            }
                        }
                        break;
                    case 'range':
                        //范围匹配
                        if (!empty($item['field']) && (!empty($item['value'] || 0 === $item['value'])) && !empty($item['expression'])) {
                            $query['bool']['must'][] = ['range' => [$item['field'] => [$item['expression'] => $item['value']]]];
                        }
                        break;
                    case 'wildcard':
                        //模糊匹配（正则）
                        if (!empty($item['field']) && (!empty($item['value']) || 0 === $item['value'] || '0' === $item['value'])) {
                            if (empty($item['position'])) {
                                $query['bool']['must'][] = ['wildcard' => [$item['field'] => "*{$item['value']}*"]];
                            } else {
                                if ('front' === $item['position']) {
                                    $query['bool']['must'][] = ['wildcard' => [$item['field'] => "{$item['value']}*"]];
                                } elseif ('later' === $item['position']) {
                                    $query['bool']['must'][] = ['wildcard' => [$item['field'] => "*{$item['value']}"]];
                                }
                            }
                        }
                        break;
                    case 'must_not':
                        //不匹配
                        if (!empty($item['field']) && (!empty($item['value']) || 0 === $item['value'])) {
                            $query['bool']['must_not'][] = ['match' => [$item['field'] => $item['value']]];
                        }
                        break;
                    case 'must_not_term':
                        if (!empty($item['field']) && (!empty($item['value'] || 0 === $item['value']))) {
                            $query['bool']['must_not'][] = ['term' => [$item['field'] => $item['value']]];
                        }
                        break;
                    case 'multi_must_not':
                        //不匹配多个
                        $values = explode(',', $item['value']);
                        if (!empty($values) && is_array($values)) {
                            foreach ($values as $val) {
                                $query['bool']['must_not'][] = ['match' => [$item['field'] => $val]];
                            }
                        }
                        break;
                    case 'multi_must_not_term':
                        $values = explode(',', $item['value']);
                        if (!empty($values) && is_array($values)) {
                            foreach ($values as $val) {
                                $query['bool']['must_not'][] = ['term' => [$item['field'] => $val]];
                            }
                        }
                        break;
                    case 'should':
                        //多个选项匹配
                        $values = explode(',', $item['value']);
                        $bool = [];
                        if (!empty($values) && is_array($values)) {
                            foreach ($values as $val) {
                                $bool['bool']['should'][] = ['match' => [$item['field'] => $val]];
                            }
                            $bool['bool']['minimum_should_match'] = 1; //至少匹配一项
                            $query['bool']['must'][] = $bool;
                        }
                        break;
                    case 'should_term':
                        $values = explode(',', $item['value']);
                        $bool = [];
                        if (!empty($values) && is_array($values)) {
                            foreach ($values as $val) {
                                $bool['bool']['should'][] = ['term' => [$item['field'] => $val]];
                            }
                            $bool['bool']['minimum_should_match'] = 1; //至少匹配一项
                            $query['bool']['must'][] = $bool;
                        }
                        break;
                    case 'should_not_term':
                        $values = explode(',', $item['value']);
                        $bool = [];
                        if (!empty($values) && is_array($values)) {
                            foreach ($values as $val) {
                                $bool['bool']['should'][] = ['bool' => ['must_not' => [['term' => [$item['field'] => $val]]]]];
                            }
                            $bool['bool']['minimum_should_match'] = 1; //至少匹配一项
                            $query['bool']['must'][] = $bool;
                        }
                        break;
                    case 'or':
                        //或者
                        if (!empty($item['field']) && (!empty($item['value'] || 0 === $item['value']))) {
                            $query['bool']['should'][] = ['match' => [$item['field'] => $item['value']]];
                        }
                        break;
                    case 'not_exists':
                        if (!empty($item['field'])) {
                            $query['bool']['must_not'][] = ['exists' => ['field' => $item['field']]];
                        }
                        break;
                    case 'exists':
                        if (!empty($item['field'])) {
                            $query['bool']['must'][] = ['exists' => ['field' => $item['field']]];
                        }
                        break;
                    case 'between':
                        $start = isset($item['value']['start']) ? $item['value']['start'] : '';
                        $end = isset($item['value']['end']) ? $item['value']['end'] : '';
                        if ((!empty($start) || '0' === $start || 0 === $start) && (!empty($end) || 0 === $end || '0' === $end)) {
                            $query['bool']['must'][] = ['range' => [$item['field'] => ['gte' => $start]]];
                            $query['bool']['must'][] = ['range' => [$item['field'] => ['lte' => $end]]];
                        }
                        break;
                    case 'not_between':
                        $start = isset($item['value']['start']) ? $item['value']['start'] : '';
                        $end = isset($item['value']['end']) ? $item['value']['end'] : '';
                        $bool = [];
                        if ((!empty($start) || '0' === $start || 0 === $start) && (!empty($end) || 0 === $end || '0' === $end)) {
                            $bool['bool']['should'][] = ['range' => [$item['field'] => ['gt' => $end]]];
                            $bool['bool']['should'][] = ['range' => [$item['field'] => ['lt' => $start]]];
                            $bool['bool']['minimum_should_match'] = 1; //至少匹配一项
                            $query['bool']['must'][] = $bool;
                        }
                        break;
                    case 'multi_should_field_terms':
                        //多个选项聚合匹配
                        $fieldTerms = empty($item['field_terms']) ? [] : $item['field_terms'];
                        $bool = [];
                        if (!empty($fieldTerms) && is_array($fieldTerms)) {
                            foreach ($fieldTerms as $k => $fieldTerm) {
                                $oper = empty($fieldTerm['oper']) ? '' : $fieldTerm['oper'];
                                $field = empty($fieldTerm['field']) ? '' : $fieldTerm['field'];
                                $value = empty($fieldTerm['value']) ? '' : $fieldTerm['value'];
                                if (!empty($oper) && !empty($field) && (!empty($value) || 0 === $value)) {
                                    switch ($oper) {
                                        case 'match':
                                            $bool['bool']['should'][] = ['match' => [$field => $value]];
                                            break;
                                        case 'term':
                                            $bool['bool']['should'][] = ['term' => [$field => $value]];
                                            break;
                                        case 'range':
                                            $expression = empty($fieldTerm['expression']) ? '' : $fieldTerm['expression'];
                                            if (!empty($expression) && in_array($expression, ['gt', 'gte', 'lt', 'lte'])) {
                                                $bool['bool']['should'][] = ['range' => [$field => [$expression => $value]]];
                                            }
                                            break;
                                    }
                                }
                            }
                            $bool['bool']['minimum_should_match'] = 1; //至少聚合匹配一项
                            $query['bool']['must'][] = $bool;
                        }
                        break;
                }
            }
        }

        return $query;
    }
}
