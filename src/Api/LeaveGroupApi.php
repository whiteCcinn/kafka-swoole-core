<?php

namespace Kafka\Api;

use Kafka\Enum\ProtocolErrorEnum;
use Kafka\Kafka;
use Kafka\Protocol\Request\LeaveGroupRequest;
use Kafka\Protocol\Response\LeaveGroupResponse;
use Kafka\Protocol\Type\String16;

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
        $socket = Kafka::getInstance()->getOffsetConnectSocket();
        $socket->send($data);
        $socket->revcByKafka($protocol);
        /** @var LeaveGroupResponse $responses */
        $responses = $protocol->response;
        if ($responses->getErrorCode()->getValue() !== ProtocolErrorEnum::NO_ERROR) {
            throw new \RuntimeException(sprintf('%s leave group %s fail', $memberId, $groupId));
        }

        return true;
    }
}