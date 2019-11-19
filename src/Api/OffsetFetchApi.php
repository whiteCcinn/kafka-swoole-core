<?php

namespace Kafka\Api;

use Kafka\ClientKafka;
use Kafka\Enum\ProtocolErrorEnum;
use Kafka\Enum\ProtocolVersionEnum;
use Kafka\Exception\ClientException;
use Kafka\Exception\RequestException\OffsetFetchRequestException;
use Kafka\Protocol\Request\OffsetFetch\PartitionsOffsetFetch;
use Kafka\Protocol\Request\OffsetFetch\TopicsOffsetFetch;
use Kafka\Protocol\Request\OffsetFetchRequest;
use Kafka\Protocol\Response\OffsetFetchResponse;
use Kafka\Protocol\Type\Int16;
use Kafka\Protocol\Type\Int32;
use Kafka\Protocol\Type\String16;
use Kafka\Socket\Socket;
use App\App;

class OffsetFetchApi extends AbstractApi
{
    private $host;

    private $port;

    /**
     * @return mixed
     */
    public function getHost()
    {
        return $this->host;
    }

    /**
     * @param mixed $host
     *
     * @return OffsetFetchApi
     */
    public function setHost($host)
    {
        $this->host = $host;

        return $this;
    }

    /**
     * @return mixed
     */
    public function getPort()
    {
        return $this->port;
    }

    /**
     * @param mixed $port
     *
     * @return OffsetFetchApi
     */
    public function setPort($port)
    {
        $this->port = $port;

        return $this;
    }

    public function getOffsetByGroupAndTopicAndPartitions(string $groupId, array $allPartitions): array
    {
        $offsetFetchRequest = new OffsetFetchRequest();
        $offsetFetchRequest->setGroupId(String16::value($groupId));
        $setTopics = [];
        foreach ($allPartitions as $topic => $partitions) {
            $topicsOffsetFetch = (new TopicsOffsetFetch())->setTopic(String16::value($topic));
            $setPartitions = [];
            foreach ($partitions as $partition) {
                $setPartitions[] = (new PartitionsOffsetFetch())->setPartition(Int32::value($partition));
            }
            $setTopics[] = $topicsOffsetFetch->setPartitions($setPartitions);
        }
        $offsetFetchRequest->setTopics($setTopics);
        $data = $offsetFetchRequest->pack();
        $socket = new Socket();
        $socket->connect($this->getHost(), $this->getPort())->send($data);
        $socket->revcByKafka($offsetFetchRequest);

        /** @var OffsetFetchResponse $response */
        $response = $offsetFetchRequest->response;
        $result = [];
        try {
            $needChangeApiVersion = true;
            foreach ($response->getResponses() as $response) {
                foreach ($response->getPartitionResponses() as $partitionResponse) {
                    // need change api version

                    $offset = $partitionResponse->getOffset()->getValue();

                    if ($offset >= 0) {
                        $needChangeApiVersion = false;
                        if ($partitionResponse->getErrorCode()->getValue() !== ProtocolErrorEnum::NO_ERROR) {
                            throw new OffsetFetchRequestException(sprintf('Api Version 0, OffsetFetchRequest request error, the error message is: %s',
                                ProtocolErrorEnum::getTextByCode($partitionResponse->getErrorCode()->getValue())));
                        }
                    }

                    $result[] = [
                        'topic'     => $response->getTopic()->getValue(),
                        'partition' => $partitionResponse->getPartition()->getValue(),
                        'offset'    => $partitionResponse->getOffset()->getValue()
                    ];
                }
            }
            if ($needChangeApiVersion) {
                throw new ClientException(
                    sprintf('Offset does not exist in zookeeper, but in kafka. Therefore, API version needs to be changed')
                );
            }
        } catch (ClientException $exception) {
            $offsetFetchRequest->getRequestHeader()
                               ->setApiVersion(Int16::value(ProtocolVersionEnum::API_VERSION_1));
            $data = $offsetFetchRequest->pack();
            $socket->send($data);
            $socket->revcByKafka($offsetFetchRequest);
            /** @var OffsetFetchResponse $response */
            $response = $offsetFetchRequest->response;
            foreach ($response->getResponses() as $response) {
                foreach ($response->getPartitionResponses() as $partitionResponse) {
                    if ($partitionResponse->getErrorCode()->getValue() !== ProtocolErrorEnum::NO_ERROR) {
                        throw new OffsetFetchRequestException(sprintf('Api Version 1, OffsetFetchRequest request error, the error message is: %s',
                            ProtocolErrorEnum::getTextByCode($partitionResponse->getErrorCode()->getValue())));
                    }

                    $offset = $partitionResponse->getOffset()->getValue();

                    ClientKafka::getInstance()
                               ->getTopicPartitionListOffsets(
                                   $response->getTopic()
                                            ->getValue(),
                                   $partitionResponse->getPartition()
                                                     ->getValue()
                               );
                    if ($offset === -1) {
                        ['offset' => $offset, 'highWatermark' => $highWatermark] = ClientKafka::getInstance()
                                                                                              ->getTopicPartitionListOffsets(
                                                                                                  $response->getTopic()
                                                                                                           ->getValue(),
                                                                                                  $partitionResponse->getPartition()
                                                                                                                    ->getValue()
                                                                                              );

                        if (App::$commonConfig->getAutoOffsetReset() === OffsetResetEnum::getTextByCode(OffsetResetEnum::LARGEST)) {
                            $offset = $highWatermark - 1;
                        }
                    }
                    $result[] = [
                        'topic'     => $response->getTopic()->getValue(),
                        'partition' => $partitionResponse->getPartition()->getValue(),
                        'offset'    => $partitionResponse->getOffset()->getValue()
                    ];
                }
            }
        }
        $socket->close();

        return $result;
    }
}