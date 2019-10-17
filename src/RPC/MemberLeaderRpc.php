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
     * @return string
     */
    public function getMemberLeaderId(): string
    {
        if (ClientKafka::getInstance()->isLeader()) {
            return ClientKafka::getInstance()->getMemberId();
        } else {
            return '';
        }
    }

    /**
     * @param array $multipleInfos
     *
     * @return null|string
     */
    public function onGetMemberLeaderId(array $multipleInfos): ?string
    {
        foreach ($multipleInfos as $infos) {
            if (!empty($infos)) {
                return $infos;
            }
        }

        return null;
    }
}