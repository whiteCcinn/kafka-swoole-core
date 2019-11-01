<?php
declare(strict_types=1);

namespace KafkaTest\Protocol;

use Kafka\Protocol\Request\DescribeGroupsRequest;
use Kafka\Protocol\Request\MetadataRequest;
use Kafka\Protocol\Type\String16;
use KafkaTest\AbstractProtocolTest;

final class DescribeGroupTest extends AbstractProtocolTest
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
     * @group  encode
     * @return string
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
     * @group   decode
     * @throws \Kafka\Exception\ProtocolTypeException
     * @throws \ReflectionException
     */
    public function testDecode()
    {

        /** @var MetadataRequest $protocol */
        $protocol = $this->protocol;
        $buffer = '000000d40000000f000000010000000c6b61666b612d73776f6f6c650006537461626c650008636f6e73756d6572000552616e67650000000100316b61666b612d73776f6f6c652d39333638633736352d393165362d346232322d626363312d393732306234333238663530000c6b61666b612d73776f6f6c65000c2f3139322e3136372e382e320000001a000000000001000e6d756c6f675f636c65616e5f3234000000000000002e000000000001000e6d756c6f675f636c65616e5f3234000000040000000000000001000000020000000300000000';

        $response = $protocol->response;
        $response->unpack(hex2bin($buffer));

        $expected = [
            'groups'         =>
                [
                    0 =>
                        [
                            'errorCode'    => 0,
                            'groupId'      => 'kafka-swoole',
                            'groupState'   => 'Stable',
                            'protocolType' => 'consumer',
                            'protocolData' => 'Range',
                            'members'      =>
                                [
                                    0 =>
                                        [
                                            'memberId'         => 'kafka-swoole-9368c765-91e6-4b22-bcc1-9720b4328f50',
                                            'clientId'         => 'kafka-swoole',
                                            'clientHost'       => '/192.167.8.2',
                                            'memberMetadata'   =>
                                                [
                                                    'version'  => 0,
                                                    'topics'   =>
                                                        [
                                                            0 => 'mulog_clean_24',
                                                        ],
                                                    'userData' => '',
                                                ],
                                            'memberAssignment' =>
                                                [
                                                    'version'              => 0,
                                                    'partitionsAssignment' =>
                                                        [
                                                            0 =>
                                                                [
                                                                    'topic'      => 'mulog_clean_24',
                                                                    'partitions' =>
                                                                        [
                                                                            0 => 0,
                                                                            1 => 1,
                                                                            2 => 2,
                                                                            3 => 3,
                                                                        ],
                                                                ],
                                                        ],
                                                    'userData'             => '',
                                                ],
                                        ],
                                ],
                        ],
                ],
            'responseHeader' =>
                [
                    'correlationId' => 15,
                ],
            'size'           => 212,
        ];
        $this->assertEquals($expected, $response->toArray());
    }
}
