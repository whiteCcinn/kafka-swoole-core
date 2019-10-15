<?php

namespace Kafka\Api;

use Kafka\Enum\ProtocolErrorEnum;
use Kafka\Kafka;
use Kafka\Protocol\Request\LeaveGroupRequest;
use Kafka\Protocol\Response\LeaveGroupResponse;
use Kafka\Protocol\Type\String16;
use Kafka\Socket\Socket;

class LeaveGroupApi extends AbstractApi
{
    /**
     * @param string $groupId
     * @param string $memberId
     *
     * @return bool
     */
    public function leave(string $groupId, string $memberId): bool
    {
        $protocol = new LeaveGroupRequest();
        $protocol->setGroupId(String16::value($groupId))->setMemberId(String16::value($memberId));
        $data = $protocol->pack();
        foreach (Kafka::getInstance()->getBrokers() as $info) {
            ['host' => $host, 'port' => $port] = $info;
            $socket = new Socket();
            $socket->connect($host, $port)->send($data);
            $socket->revcByKafka($protocol);
            $socket->close();
            /** @var LeaveGroupResponse $responses */
            $responses = $protocol->response;
            if ($responses->getErrorCode()->getValue() === ProtocolErrorEnum::NO_ERROR) {
                break;
            }
        }

        return true;
    }
}