<?php
declare(strict_types=1);

namespace Kafka\RPC;

use Kafka\Api\ListOffsetsApi;
use Kafka\Api\OffsetFetchApi;
use Kafka\ClientKafka;

/**
 * Class KafkaLagRpc
 */
class KafkaOffsetRpc extends BaseRpc
{
    public function getKafkaLag(): array
    {
        $result = [];
        foreach (ClientKafka::getInstance()->getSelfTopicPartition() as $topic => $partitions) {
            foreach ($partitions as $partition) {
                $offset = ClientKafka::getInstance()->getTopicPartitionOffsetByTopicPartition($topic, $partition);
                $ret = ListOffsetsApi::getInstance()->getListOffsets($topic, [$partition]);
                $item = [
                    'topic'     => $topic,
                    'partition' => $partition,
                    'offset'    => $offset,
                ];
                foreach ($ret as $info) {
                    if ($info['topic'] === $topic && $info['partition'] === $partition) {
                        $item['earliest'] = $info['earliest'];
                        $item['highWatermark'] = $info['highWatermark'];
                    }
                }
                $result[] = $item;
            }
        }

        return $result;
    }

    public function onGetKafkaLag(array $multipleInfos): int
    {
        $offset = 0;
        $latest = 0;
        foreach ($multipleInfos as $infos) {
            foreach ($infos as $info) {
                $offset += $info['offset'];
                $latest += $info['highWatermark'] - 1;
            }
        }

        $kafkaLag = $latest - $offset;

        return $kafkaLag;
    }

    public function getOffsetChecker()
    {
        $result = $this->getKafkaLag();

        return $result;
    }

    public function onGetOffsetChecker(array $multipleInfos): array
    {
        $result = [];
        foreach ($multipleInfos as $infos) {
            foreach ($infos as $info) {
                $result[] = [
                    'topic'     => $info['topic'],
                    'partition' => $info['partition'],
                    'offset'    => $offset = $info['offset'],
                    'latest'    => $highWatermark = $info['highWatermark'] - 1,
                    'diff'      => $highWatermark - $offset
                ];
            }
        }

        return $result;
    }
}