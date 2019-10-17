<?php

namespace Kafka\Api;

use App\App;
use Kafka\ClientKafka;
use Kafka\Enum\ListOffsetsTimestampEnum;
use Kafka\Enum\ProtocolErrorEnum;
use Kafka\Exception\RequestException\ListOffsetsRequestException;
use Kafka\Exception\RequestException\OffsetCommitRequestException;
use Kafka\Kafka;
use Kafka\Protocol\Request\ListOffsets\PartitionsListsOffsets;
use Kafka\Protocol\Request\ListOffsets\TopicsListsOffsets;
use Kafka\Protocol\Request\ListOffsetsRequest;
use Kafka\Protocol\Request\OffsetCommit\PartitionsOffsetCommit;
use Kafka\Protocol\Request\OffsetCommit\TopicsOffsetCommit;
use Kafka\Protocol\Request\OffsetCommitRequest;
use Kafka\Protocol\Response\ListOffsetsResponse;
use Kafka\Protocol\Response\OffsetCommitResponse;
use Kafka\Protocol\Type\Int32;
use Kafka\Protocol\Type\Int64;
use Kafka\Protocol\Type\String16;
use Kafka\Socket\Socket;

class ListOffsetsApi extends AbstractApi
{
    /**
     * @param string $topic
     * @param int    $partition
     * @param int    $offset
     *
     * @throws OffsetCommitRequestException
     */
    public function getListOffsets(string $topic, array $partitions, int $timestamp = ListOffsetsTimestampEnum::LATEST,
                                   int $maxNumOffsets = 99999999)
    {
        $listOffsetsRequest = new ListOffsetsRequest();

        $setPartitions = [];
        $result = [];
        foreach ($partitions as $partition) {
            $leaderId = Kafka::getInstance()->getTopicsPartitionLeaderByTopicAndPartition($topic, $partition);
            ['host' => $host, 'port' => $port] = Kafka::getInstance()->getBrokerInfoByNodeId($leaderId);
            $setPartitions[] = (new PartitionsListsOffsets())->setPartition(Int32::value($partition))
                                                             ->setTimestamp(Int64::value($timestamp))
                                                             ->setMaxNumOffsets(Int32::value($maxNumOffsets));

            $topicListsOffsets = (new TopicsListsOffsets())->setTopic(String16::value($topic));
            $listOffsetsRequest->setTopics([
                (new TopicsListsOffsets())->setTopic(String16::value($topic))
                                          ->setPartitions($setPartitions)
            ])->setReplicaId(Int32::value(-1));

            $data = $listOffsetsRequest->pack();
            $socket = new Socket();
            $socket->connect($host, $port)->send($data);
            $socket->revcByKafka($listOffsetsRequest);

            /** @var ListOffsetsResponse $response */
            $response = $listOffsetsRequest->response;
            foreach ($response->getResponses() as $response) {
                foreach ($response->getPartitionResponses() as $partitionResponse) {
                    if ($partitionResponse->getErrorCode()->getValue() !== ProtocolErrorEnum::NO_ERROR) {
                        throw new ListOffsetsRequestException(sprintf('ListOffsets request error, the error message is: %s',
                            ProtocolErrorEnum::getTextByCode($partition->getErrorCode()->getValue())));
                    }
                    if (count($partitionResponse->getOffsets()) > 1) {
                        [$highWatermark, $offset] = $partitionResponse->getOffsets();
                        $offset = $offset->getValue();
                        $highWatermark = $highWatermark->getValue();
                    } else {
                        $offset = current($partitionResponse->getOffsets())->getValue();
                        $highWatermark = $offset;
                    }

                    $result[] = [
                        'topic'         => $topic,
                        'partition'     => $partitionResponse->getPartition()->getValue(),
                        'earliest'      => $offset,
                        'highWatermark' => $highWatermark,
                    ];
                }
            }
        }

        return $result;
    }
}