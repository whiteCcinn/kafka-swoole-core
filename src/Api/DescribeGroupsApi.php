<?php

namespace Kafka\Api;

use Kafka\ClientKafka;
use Kafka\Enum\ProtocolErrorEnum;
use Kafka\Exception\RequestException\DescribeGroupsRequestException;
use Kafka\Kafka;
use Kafka\Protocol\Request\DescribeGroupsRequest;
use Kafka\Protocol\Response\DescribeGroupsResponse;
use Kafka\Protocol\Type\String16;
use Kafka\Socket\Socket;

class DescribeGroupsApi extends AbstractApi
{
    /**
     * @param string $host
     * @param int    $port
     * @param string $groupId
     *
     * @return bool
     * @throws DescribeGroupsRequestException
     * @throws \Kafka\Exception\Socket\NormalSocketConnectException
     */
    public function describe(string $host, int $port, string $groupId): array
    {
        $describeGroupsRequest = new DescribeGroupsRequest();
        $describeGroupsRequest->setGroups([String16::value($groupId)]);
        $data = $describeGroupsRequest->pack();
        $socket = new Socket();
        $socket->connect($host, $port)->send($data);
        $socket->revcByKafka($describeGroupsRequest);

        /** @var DescribeGroupsResponse $response */
        $response = $describeGroupsRequest->response;
        foreach ($response->getGroups() as $response) {
            if ($response->getErrorCode()->getValue() !== ProtocolErrorEnum::NO_ERROR) {
                throw new DescribeGroupsRequestException(sprintf('DescribeGroups request error, the error message is: %s',
                    ProtocolErrorEnum::getTextByCode($partition->getErrorCode()->getValue())));
            }
        }

        return $response->toArray();
    }
}