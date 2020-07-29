<?php
namespace Es\Common;
/**
 * 
 */
use Elasticsearch\ClientBuilder;

trait EsTrait
{
    /**
     * connect times
     * @var integer
     */
    private $connetEsTimes = 0;

    /**
     * max connect times
     * @var integer
     */
    private $MaxConnectEsTimes = 3;

	/**
     * 获取ES连接
     *
     * @return \Elasticsearch\Client
     */
    public function connectElasticsearch()
    {
        //获取elasticsearch全文检索主机
        $esHosts = $this->config['host'];

        try {
            $clientBuilder = ClientBuilder::create();
            $clientBuilder->setHosts($esHosts);
            $clientBuilder->setRetries(1);
            $clientBuilder->setLogger($this->logger);
            $esClient = $clientBuilder->build();
            
            if ($esClient->ping()) {
                $this->logger->info(sprintf('连接Elasticsearch成功[%s]', json_encode($esHosts)));
            }
        } catch (\Exception $e) {
            $this->connetEsTimes++;

            if ($this->connetEsTimes > $this->MaxConnectEsTimes) {
            	return false;
            }

            $this->logger->error(sprintf(
                "连接Elasticsearch失败,失败次数为[%s],失败原因为[%s].",
                $this->connetEsTimes,
                $e->getMessage()
            ));
            sleep($this->connetEsTimes*5);

            return $this->connectElasticsearch();
        }

        return $esClient;
    }
}
