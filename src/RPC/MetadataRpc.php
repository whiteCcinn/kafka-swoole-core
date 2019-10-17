<?php
declare(strict_types=1);

namespace Kafka\RPC;

use Kafka\ClientKafka;
use Kafka\Kafka;

/**
 * Class MetadataRpc
 *
 * @package Kafka\RPC
 */
class MetadataRpc extends BaseRpc
{
    /**
     * @return array
     */
    public function getBrokerList(): array
    {
        return Kafka::getInstance()->getBrokers();
    }

    /**
     * @param array $multipleInfos
     *
     * @return array
     */
    public function onGetBrokerList(array $multipleInfos): array
    {
        $result = [];
        foreach ($multipleInfos as $infos) {
            $result = $result + $infos;
        }

        return $result;
    }

    /**
     * @return array
     */
    public function getTopics(): array
    {
        return Kafka::getInstance()->getTopics();
    }

    /**
     * @param array $multipleInfos
     *
     * @return array
     */
    public function onGetTopics(array $multipleInfos): array
    {
        $result = [];
        foreach ($multipleInfos as $infos) {
            $result = $result + $infos;
        }

        return $result;
    }
}