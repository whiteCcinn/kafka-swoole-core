<?php
declare(strict_types=1);

namespace KafkaTest\Protocol;

use Kafka\Protocol\Request\Produce\DataProduce;
use Kafka\Protocol\Request\Produce\MessageProduce;
use Kafka\Protocol\Request\Produce\MessageSetProduce;
use Kafka\Protocol\Request\Produce\TopicDataProduce;
use Kafka\Protocol\Request\ProduceRequest;
use Kafka\Protocol\Type\Bytes32;
use Kafka\Protocol\Type\Int16;
use Kafka\Protocol\Type\Int32;
use Kafka\Protocol\Type\Int64;
use Kafka\Protocol\Type\String16;
use KafkaTest\AbstractProtocolTest;

final class ProduceTest extends AbstractProtocolTest
{
    /**
     * @var ProduceRequest $protocol
     */
    private $protocol;

    /**
     * @before
     */
    public function newProtocol()
    {
        $this->protocol = new ProduceRequest();
    }

    /**
     * @author caiwenhui
     * @group  encode
     * @throws \Kafka\Exception\ProtocolTypeException
     * @throws \ReflectionException
     */
    public function testEncode()
    {
        /** @var ProduceRequest $protocol */
        $protocol = $this->protocol;
        $protocol->setAcks(Int16::value(1))
                 ->setTimeout(Int32::value(1 * 1000))
                 ->setTopicData([
                     (new TopicDataProduce())->setTopic(String16::value('caiwenhui'))
                                             ->setData([
                                                 (new DataProduce())->setPartition(Int32::value(0))
                                                                    ->setMessageSet([
                                                                        (new MessageSetProduce())->setOffset(Int64::value(0))
                                                                                                 ->setMessage(
                                                                                                     (new MessageProduce())->setValue(Bytes32::value('test...'))
                                                                                                 ),

                                                                    ])
                                             ])
                 ]);
        $data = $protocol->pack();

        $expected = '000000580000000000000000000c6b61666b612d73776f6f6c650001000003e800000001000963616977656e6875690000000100000000000000210000000000000000000000153c1950a800000000000000000007746573742e2e2e';
        $this->assertSame($expected, bin2hex($data));

        return $data;
    }

    /**
     * @author caiwenhui
     * @group  encode
     * @throws \Kafka\Exception\ProtocolTypeException
     * @throws \ReflectionException
     */
    public function testMultiEncode()
    {
        /** @var ProduceRequest $protocol */
        $protocol = $this->protocol;
        $protocol->setAcks(Int16::value(1))
                 ->setTimeout(Int32::value(1 * 1000))
                 ->setTopicData([
                     (new TopicDataProduce())->setTopic(String16::value('caiwenhui'))
                                             ->setData([
                                                 (new DataProduce())->setPartition(Int32::value(0))
                                                                    ->setMessageSet([
                                                                        (new MessageSetProduce())->setOffset(Int64::value(0))
                                                                                                 ->setMessage(
                                                                                                     (new MessageProduce())->setValue(Bytes32::value('test1...'))
                                                                                                 ),
                                                                        (new MessageSetProduce())->setOffset(Int64::value(1))
                                                                                                 ->setMessage(
                                                                                                     (new MessageProduce())->setValue(Bytes32::value('test2...'))
                                                                                                 ),

                                                                    ])
                                             ])
                 ]);
        $data = $protocol->pack();

        $expected = '0000007b0000000000000000000c6b61666b612d73776f6f6c650001000003e800000001000963616977656e687569000000010000000000000044000000000000000000000016012658d00000000000000000000874657374312e2e2e0000000000000001000000161393f73e0000000000000000000874657374322e2e2e';
        $this->assertSame($expected, bin2hex($data));

        return $data;
    }

    /**
     * @author  caiwenhui
     * @group   decode
     */
    public function testDecode()
    {
        /** @var ProduceRequest $protocol */
        $protocol = $this->protocol;
        $buffer = '000000250000000000000001000963616977656e687569000000010000000000000000000000000068';

        $response = $protocol->response;
        $response->unpack(hex2bin($buffer));

        $expected = [
            'responses'      =>
                [
                    0 =>
                        [
                            'topic'               => 'caiwenhui',
                            'partition_responses' =>
                                [
                                    0 =>
                                        [
                                            'partition'  => 0,
                                            'errorCode'  => 0,
                                            'baseOffset' => 104,
                                        ],
                                ],
                        ],
                ],
            'responseHeader' =>
                [
                    'correlationId' => 0,
                ],
            'size'           => 37,
        ];
        $this->assertEquals($expected, $response->toArray());
    }

    /**
     * @author  caiwenhui
     * @group   decode
     */
    public function testDecodeMulti()
    {
        /** @var ProduceRequest $protocol */
        $protocol = $this->protocol;
        $buffer = '000000250000000000000001000963616977656e687569000000010000000000000000000000000069';

        $response = $protocol->response;
        $response->unpack(hex2bin($buffer));

        $expected = [
            'responses'      =>
                [
                    0 =>
                        [
                            'topic'               => 'caiwenhui',
                            'partition_responses' =>
                                [
                                    0 =>
                                        [
                                            'partition'  => 0,
                                            'errorCode'  => 0,
                                            'baseOffset' => 105,
                                        ],
                                ],
                        ],
                ],
            'responseHeader' =>
                [
                    'correlationId' => 0,
                ],
            'size'           => 37,
        ];
        $this->assertEquals($expected, $response->toArray());
    }
}
