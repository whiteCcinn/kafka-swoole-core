<?php
declare(strict_types=1);

namespace Kafka\RPC;

use Kafka\ClientKafka;

/**
 * Class MemberLeaderRpc
 *
 * @package Kafka\RPC
 */
class MemberLeaderRpc extends BaseRpc
{
    /**
     * @return array
     */
    public function getMemberLeaderId(): array
    {
        $result['memberId'] = ClientKafka::getInstance()->getMemberId();
        if (ClientKafka::getInstance()->isLeader()) {
            $result['isLeader'] = true;
        } else {
            $result['isLeader'] = false;
        }

        return $result;
    }

    /**
     * @param array $multipleInfos
     *
     * @return array
     */
    public function onGetMemberLeaderId(array $multipleInfos): array
    {
        return $multipleInfos;
    }
}