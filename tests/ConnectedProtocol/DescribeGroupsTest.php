<?php
declare(strict_types=1);

namespace KafkaTest\ConnectedProtocol;

use Kafka\Enum\ProtocolErrorEnum;
use Kafka\Protocol\Request\DescribeGroupsRequest;
use Kafka\Protocol\Request\Produce\DataProduce;
use Kafka\Protocol\Request\Produce\MessageProduce;
use Kafka\Protocol\Request\Produce\MessageSetProduce;
use Kafka\Protocol\Request\Produce\TopicDataProduce;
use Kafka\Protocol\Request\ProduceRequest;
use Kafka\Protocol\Response\DescribeGroups\GroupsDescribeGroups;
use Kafka\Protocol\Response\DescribeGroupsResponse;
use Kafka\Protocol\Type\Bytes32;
use Kafka\Protocol\Type\Int16;
use Kafka\Protocol\Type\Int32;
use Kafka\Protocol\Type\Int64;
use Kafka\Protocol\Type\String16;
use Kafka\Server\SocketServer;
use KafkaTest\AbstractProtocolTest;
use Swoole\Client;


final class DescribeGroupsTest extends AbstractProtocolTest
{
    /**
     * @var DescribeGroupsRequest $protocol
     */
    private $protocol;

    /**
     * @before
     */
    public function newProtocol()
    {
        $this->protocol = new DescribeGroupsRequest();
    }

    /**
     * @author caiwenhui
     * @group  connectedEncode
     * @throws \Kafka\Exception\ProtocolTypeException
     * @throws \ReflectionException
     */
    public function testEncode()
    {
        /** @var DescribeGroupsRequest $protocol */
        $protocol = $this->protocol;
        $protocol->setGroups([
            String16::value('kafka-swoole')
        ]);
        $data = $protocol->pack();

        $expected = '00000028000f00000000000f000c6b61666b612d73776f6f6c6500000001000c6b61666b612d73776f6f6c65';
        $this->assertSame($expected, bin2hex($data));

        return $data;
    }

    /**
     * @author  caiwenhui
     * @depends testEncode
     *
     * @param string $data
     */
    public function testSend(string $data)
    {
        /** @var ProduceRequest $protocol */
        $protocol = $this->protocol;
        $data = SocketServer::getInstance()->run('mkafka3', 9092, function () use ($data) {
            return $data;
        }, function (string $data, Client $client) use ($protocol) {
            $protocol->response->unpack($data, $client);
        });

        /** @var DescribeGroupsResponse $response */
        $response = $protocol->response;
        foreach ($response->getGroups() as $resp) {
                $this->assertEquals(ProtocolErrorEnum::NO_ERROR, $resp->getErrorCode()->getValue());
        }
    }
}
