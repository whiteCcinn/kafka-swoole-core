<?php

namespace Kafka\Api;

use http\Exception\RuntimeException;
use Kafka\Enum\CompressionCodecEnum;
use Kafka\Enum\ProtocolErrorEnum;
use Kafka\Kafka;
use Kafka\Log\KafkaLog;
use Kafka\Protocol\Request\Produce\DataProduce;
use Kafka\Protocol\Request\Produce\MessageProduce;
use Kafka\Protocol\Request\Produce\MessageSetProduce;
use Kafka\Protocol\Request\Produce\TopicDataProduce;
use Kafka\Protocol\Request\ProduceRequest;
use Kafka\Protocol\Response\ProduceResponse;
use Kafka\Protocol\Type\Bytes32;
use Kafka\Protocol\Type\Int16;
use Kafka\Protocol\Type\Int32;
use Kafka\Protocol\Type\Int64;
use Kafka\Protocol\Type\Int8;
use Kafka\Protocol\Type\String16;
use Kafka\Socket\Socket;

class ProducerApi extends AbstractApi
{
    /**
     * @var array
     */
    private $connBrokerListMap = [];

    /**
     * ['conn'=>['topic' => ['partition'=>'leaderId']]]
     *
     * @var array
     */
    private $metadata = [];

    /**
     * @var array
     */
    private $brokers = [];

    /**
     * @param string $conn
     * @param string $topics
     *
     * @return bool
     */
    private function refreshMetadata(string $conn, string $topics): bool
    {
        $topics = array_unique(array_merge(explode(',', $topics),
            isset($this->metadata[$conn]['topicPartitionLeader']) ? (array_keys($this->metadata[$conn]['topicPartitionLeader']) ?? []) : []));
        $topics = implode(',', $topics);

        $result = MetadataApi::getInstance()->requestMetadata($this->connBrokerListMap[$conn], $topics);
        $result = array_filter($result);
        if (empty($result)) {
            return false;
        }

        [
            'partitions'           => $topicPartitions,
            'topicPartitionLeader' => $topicsPartitionLeader,
            'brokers'              => $brokers,
        ] = $result;

        $this->brokers[$conn]['brokers'] = $brokers;

        foreach ($topicPartitions as $topic => $partitions) {
            foreach ($partitions as $partition) {
                $leaderId = $topicsPartitionLeader[$topic][$partition];
                $this->metadata[$conn]['topicPartitionLeader'][$topic][$partition] = $leaderId;
            }
        }

        return true;
    }

    /**
     * @param string $conn
     * @param string $brokerList
     */
    public function setBrokerListMap(string $conn, string $brokerList)
    {
        $this->connBrokerListMap[$conn] = $brokerList;
    }

    /**
     * @param string      $conn
     * @param string      $topic
     * @param int|null    $partition
     * @param null|string $key
     * @param array       $messages
     *
     * @return bool
     */
    public function produce(string $conn, string $topic, ?int $partition, ?string $key, array $messages,
                            $compress = CompressionCodecEnum::NORMAL): bool
    {
        try {
            if (!isset($this->connBrokerListMap[$conn])) {
                return false;
            }

            Send:
            if (!(isset($this->metadata[$conn]['topicPartitionLeader'][$topic]) && !empty($this->metadata[$conn]['topicPartitionLeader'][$topic]))) {
                $bool = self::refreshMetadata($conn, $topic);
            }
            $partitions = isset($this->metadata[$conn]['topicPartitionLeader'][$topic]) ? array_keys($this->metadata[$conn]['topicPartitionLeader'][$topic]) : [];
            $topicPartitionLeaders = isset($this->metadata[$conn]['topicPartitionLeader']) ? $this->metadata[$conn]['topicPartitionLeader'] : [];
            $topicPartition = isset($partitions[$topic]) ? $partitions[$topic] : [0];
            $topicPartitionLeader = isset($topicPartitionLeaders[$topic]) ? $topicPartitionLeaders[$topic] : current($topicPartitionLeaders);
            // Range
            if ($partition === null && $key === null) {
                shuffle($topicPartition);
                $assignPartition = current($topicPartition);
            } elseif ($partition === null && $key !== null) {
                $assignPartition = crc32(md5($key)) % count($topicPartition);
            } else {
                // if ($partition !== null && $key !== null) || ($partition !== null && $key === null)
                $assignPartition = (int)$partition;
            }

            // Compress
            if ($compress === CompressionCodecEnum::NORMAL) {
                $attributes = Int8::value(CompressionCodecEnum::NORMAL);
            } elseif ($compress === CompressionCodecEnum::SNAPPY) {
                $attributes = Int8::value(CompressionCodecEnum::SNAPPY);
            } elseif ($compress === CompressionCodecEnum::GZIP) {
                $attributes = Int8::value(CompressionCodecEnum::GZIP);
            } else {
                throw new \RuntimeException('Todo Lz4 Compress');
            }

            $messageSets = [];
            foreach ($messages as $message) {
                $messageSets[] = (new MessageSetProduce())->setOffset(Int64::value(-1))
                                                          ->setMessage(
                                                              (new MessageProduce())->setAttributes($attributes)
                                                                                    ->setValue(Bytes32::value($message))
                                                          );
            }

            $protocol = new ProduceRequest();
            $protocol->setAcks(Int16::value(1))
                     ->setTimeout(Int32::value(1 * 1000))
                     ->setTopicData([
                         (new TopicDataProduce())->setTopic(String16::value($topic))
                                                 ->setData([
                                                     (new DataProduce())->setPartition(Int32::value($assignPartition))
                                                                        ->setMessageSet($messageSets)
                                                 ])
                     ]);
            $data = $protocol->pack();
            $nodeId = isset($topicPartitionLeader[$assignPartition]) ? $topicPartitionLeader[$assignPartition] : null;
            $socket = $this->getSocket($nodeId, $conn);
            $socket->send($data);
            $socket->revcByKafka($protocol);
            /** @var ProduceResponse $responses */
            $responses = $protocol->response;
            foreach ($responses->getResponses() as $response) {
                $info = $response->getPartitionResponses()[0];
                if (
                    in_array($info->getErrorCode()->getValue(),
                        [ProtocolErrorEnum::UNKNOWN_TOPIC_OR_PARTITION, ProtocolErrorEnum::NOT_LEADER_FOR_PARTITION])
                    &&
                    $info->getBaseOffset()->getValue() === -1
                ) {
                    goto Send;
                }
            }
        } catch (\Exception $e) {
            KafkaLog::getInstance()->error('Producer exception: ' . $e->getMessage());

            return false;
        } catch (\Error $error) {
            KafkaLog::getInstance()->error('Producer error: ' . $error->getMessage());

            return false;
        }

        return true;
    }

    private function getSocket($nodeId = null, string $conn)
    {
        if (empty($nodeId)) {
            $brokers = $this->brokers[$conn]['brokers'];
            ['host' => $host, 'port' => $port, 'nodeId' => $nId] = current($brokers);
            $socket = new Socket();
            $socket->connect($host, $port);
            $this->brokers[$conn]['sockets'][$nId] = $socket;
        } else {
            if (!isset($this->brokers[$conn]['sockets'][$nodeId])) {
                $brokers = $this->brokers[$conn]['brokers'];
                foreach ($brokers as $item) {
                    if ($item['nodeId'] == $nodeId) {
                        $broker = $item;
                    }
                }
                ['host' => $host, 'port' => $port, 'nodeId' => $nId] = $broker;
                $socket = new Socket();
                $socket->connect($host, $port);

                $this->brokers[$conn]['sockets'][$nId] = $socket;
            } else {
                $socket = $this->brokers[$conn]['sockets'][$nodeId];
            }
        }

        return $socket;
    }
}