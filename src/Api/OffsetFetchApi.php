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
    public function getOffsetByTopicAndPartitions(string $topic, array $partitions): array
    {
        /** @var \Kafka\Config\CommonConfig $commonConfig */
        $commonConfig = App::$commonConfig;
        $offsetFetchRequest = new OffsetFetchRequest();
        $offsetFetchRequest->setGroupId(String16::value($commonConfig->getGroupId()));
        $setPartitions = [];
        foreach ($partitions as $partition) {
            $setPartitions[] = (new PartitionsOffsetFetch())->setPartition(Int32::value($partition));
        }
        $offsetFetchRequest->setTopics([
            (new TopicsOffsetFetch())->setTopic(String16::value($topic))->setPartitions($setPartitions)
        ]);
        $data = $offsetFetchRequest->pack();
        $socket = new Socket();
        $host = ClientKafka::getInstance()->getOffsetConnectHost();
        $port = ClientKafka::getInstance()->getOffsetConnectPort();
        $socket->connect($host, $port)->send($data);
        $socket->revcByKafka($offsetFetchRequest);

        $result = [];

        /** @var OffsetFetchResponse $response */
        $response = $offsetFetchRequest->response;
        try {
            $needChangeApiVersion = [];
            foreach ($response->getResponses() as $response) {
                foreach ($response->getPartitionResponses() as $partitionResponse) {
                    // need change api version
                    if ($partitionResponse->getOffset()->getValue() === -1 && $partitionResponse->getMetadata()
                                                                                                ->getValue() === '') {
                        $needChangeApiVersion[] = true;
                    } else {
                        if ($partitionResponse->getErrorCode()->getValue() !== ProtocolErrorEnum::NO_ERROR) {
                            throw new OffsetFetchRequestException(sprintf('Api Version 0, OffsetFetchRequest request error, the error message is: %s',
                                ProtocolErrorEnum::getTextByCode($partitionResponse->getErrorCode()->getValue())));
                        }
                    }
                    $needChangeApiVersion[] = false;

                    $result[] = [
                        'topic'     => $response->getTopic()->getValue(),
                        'partition' => $partitionResponse->getPartition()->getValue(),
                        'offset'    => $partitionResponse->getOffset()->getValue()
                    ];
                }
            }
            if (count(array_unique($needChangeApiVersion)) === 1 && current($needChangeApiVersion) === true) {
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