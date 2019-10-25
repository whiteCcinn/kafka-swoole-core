<?php
declare(strict_types=1);

namespace KafkaTest\Protocol;

use Kafka\Protocol\Request\FetchRequest;
use Kafka\Protocol\Request\Fetch\PartitionsFetch;
use Kafka\Protocol\Request\Fetch\TopicsFetch;
use Kafka\Protocol\Type\Int32;
use Kafka\Protocol\Type\Int64;
use Kafka\Protocol\Type\String16;
use KafkaTest\AbstractProtocolTest;

final class FetchTest extends AbstractProtocolTest
{
    /**
     * @var FetchRequest $protocol
     */
    private $protocol;

    /**
     * @before
     */
    public function newProtocol()
    {
        $this->protocol = new FetchRequest();
    }

    /**
     * @author caiwenhui
     * @group  encode
     * @throws \Kafka\Exception\ProtocolTypeException
     * @throws \ReflectionException
     */
    public function testEncode()
    {
        /** @var FetchRequest $protocol */
        $protocol = $this->protocol;
        $protocol->setReplicaId(Int32::value(-1))
                 ->setMaxWaitTime(Int32::value(100))
                 ->setMinBytes(Int32::value(1000))
                 ->setTopics([
                     (new TopicsFetch())->setTopic(String16::value('caiwenhui'))
                                        ->setPartitions([
                                            (new PartitionsFetch())->setPartition(Int32::value(0))
                                                                   ->setFetchOffset(Int64::value(85))
                                                                   ->setPartitionMaxBytes(Int32::value(65536))
                                        ])
                 ]);

        $data = $protocol->pack();
        $expected = '000000450001000000000001000c6b61666b612d73776f6f6c65ffffffff00000064000003e800000001000963616977656e6875690000000100000000000000000000005500010000';
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
        /** @var FetchRequest $protocol */
        $protocol = $this->protocol;
        $buffer = '000000f30000000100000001000963616977656e68756900000001000000000000000000000000005b000000ca0000000000000055000000153c1950a800000000000000000007746573742e2e2e000000000000005600000016012658d00000000000000000000874657374312e2e2e0000000000000057000000161393f73e0000000000000000000874657374322e2e2e0000000000000058000000153c1950a800000000000000000007746573742e2e2e000000000000005900000016012658d00000000000000000000874657374312e2e2e000000000000005a000000161393f73e0000000000000000000874657374322e2e2e';
        $buffer = '000007f70000000100000001000d6d756c6f675f636c65616e5f30000000010000000000000000000000000010000007ca00000000000000000000009d34540d590002ffffffff0000008f82534e415050590000000001000000010000007b91010000190110856d9040a4050f00080506a00000040000006f7b2268656164657273223a7b226170705f6964223a302c226c6f675f6e616d65223a050b1474657374227d0517092a0c67656e74112c4c6964223a3232322c226e756d223a3333332c22700114483131312c227365727665725f6964223a307d7d000000000000000600000102d747d97e0002ffffffff000000f482534e41505059000000000100000001000000e0e606000009012001000000856c08264c050f00080506a00000050000006f7b2268656164657273223a7b226170705f6964223a302c226c6f675f6e616d65223a050b1474657374227d0517092a0c67656e74112c01333c3232322c226e756d223a3333332c22700114283131312c22736572766572092b047d7d0d7b000201910c6f38eb9c05101191040700fe9100e29100000301910c6732ec443291000199fe2201da2201000401910c6ea08d743291000006fe2201e62201412e108566aa8aac3291000009fe9100e69100210c10856959703c32910021c9feb301beb3010000000000000007000000a10b10838d0002ffffffff0000009382534e415050590000000001000000010000007f91010000090120070000008564022194050f00080506a000000a0000006f7b2268656164657273223a7b226170705f6964223a302c226c6f675f6e616d65223a050b1474657374227d0517092a0c67656e74112c4c6964223a3232322c226e756d223a3333332c22700114483131312c227365727665725f6964223a307d7d00000000000000080000009f6e7744ff0002ffffffff0000009182534e415050590000000001000000010000007d910100000901200800000085659a477c050f010e0101980b0000006f7b2268656164657273223a7b226170705f6964223a302c226c6f675f6e616d65223a050b1474657374227d0517092a0c67656e74112c4c6964223a3232322c226e756d223a3333332c22700114483131312c227365727665725f6964223a307d7d0000000000000009000000a176bbdfc00002ffffffff0000009382534e415050590000000001000000010000007f910100000901200900000085615377e4050f00080506a000000c0000006f7b2268656164657273223a7b226170705f6964223a302c226c6f675f6e616d65223a050b1474657374227d0517092a0c67656e74112c4c6964223a3232322c226e756d223a3333332c22700114483131312c227365727665725f6964223a307d7d000000000000000a000000a1faae6b020002ffffffff0000009382534e415050590000000001000000010000007f910100000901200a0000008560cb110c050f00080506a000000d0000006f7b2268656164657273223a7b226170705f6964223a302c226c6f675f6e616d65223a050b1474657374227d0517092a0c67656e74112c4c6964223a3232322c226e756d223a3333332c22700114483131312c227365727665725f6964223a307d7d000000000000000b000000a149e03f2d0002ffffffff0000009382534e415050590000000001000000010000007f910100000901200b000000856263ba34050f00080506a000000e0000006f7b2268656164657273223a7b226170705f6964223a302c226c6f675f6e616d65223a050b1474657374227d0517092a0c67656e74112c4c6964223a3232322c226e756d223a3333332c22700114483131312c227365727665725f6964223a307d7d000000000000000c000000a1ec5d3a280002ffffffff0000009382534e415050590000000001000000010000007f910100000901200c0000008563fbdcdc050f00080506a000000f0000006f7b2268656164657273223a7b226170705f6964223a302c226c6f675f6e616d65223a050b1474657374227d0517092a0c67656e74112c4c6964223a3232322c226e756d223a3333332c22700114483131312c227365727665725f6964223a307d7d000000000000000d000000a1b3577ec80002ffffffff0000009382534e415050590000000001000000010000007f910100000901200d000000857277b584050f00080506a00000100000006f7b2268656164657273223a7b226170705f6964223a302c226c6f675f6e616d65223a050b1474657374227d0517092a0c67656e74112c4c6964223a3232322c226e756d223a3333332c22700114483131312c227365727665725f6964223a307d7d000000000000000e000000a13f42ca0a0002ffffffff0000009382534e415050590000000001000000010000007f910100000901200e0000008573efd36c050f00080506a00000110000006f7b2268656164657273223a7b226170705f6964223a302c226c6f675f6e616d65223a050b1474657374227d0517092a0c67656e74112c4c6964223a3232322c226e756d223a3333332c22700114483131312c227365727665725f6964223a307d7d000000000000000f000000a18c0c9e250002ffffffff0000009382534e415050590000000001000000010000007f910100000901200f0000008571477854050f00080506a00000120000006f7b2268656164657273223a7b226170705f6964223a302c226c6f675f6e616d65223a050b1474657374227d0517092a0c67656e74112c4c6964223a3232322c226e756d223a3333332c22700114483131312c227365727665725f6964223a307d7d';

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
                                            'partitionHeader' =>
                                                [
                                                    'partition'     => 0,
                                                    'errorCode'     => 0,
                                                    'highWatermark' => 91,
                                                ],
                                            'recordSet'       =>
                                                [
                                                    0 =>
                                                        [
                                                            'offset'         => 85,
                                                            'messageSetSize' => 21,
                                                            'message'        =>
                                                                [
                                                                    'crc'        => 1008292008,
                                                                    'magicByte'  => 0,
                                                                    'attributes' => 0,
                                                                    'key'        => '',
                                                                    'value'      => 'test...',
                                                                ],
                                                        ],
                                                    1 =>
                                                        [
                                                            'offset'         => 86,
                                                            'messageSetSize' => 22,
                                                            'message'        =>
                                                                [
                                                                    'crc'        => 19290320,
                                                                    'magicByte'  => 0,
                                                                    'attributes' => 0,
                                                                    'key'        => '',
                                                                    'value'      => 'test1...',
                                                                ],
                                                        ],
                                                    2 =>
                                                        [
                                                            'offset'         => 87,
                                                            'messageSetSize' => 22,
                                                            'message'        =>
                                                                [
                                                                    'crc'        => 328464190,
                                                                    'magicByte'  => 0,
                                                                    'attributes' => 0,
                                                                    'key'        => '',
                                                                    'value'      => 'test2...',
                                                                ],
                                                        ],
                                                    3 =>
                                                        [
                                                            'offset'         => 88,
                                                            'messageSetSize' => 21,
                                                            'message'        =>
                                                                [
                                                                    'crc'        => 1008292008,
                                                                    'magicByte'  => 0,
                                                                    'attributes' => 0,
                                                                    'key'        => '',
                                                                    'value'      => 'test...',
                                                                ],
                                                        ],
                                                    4 =>
                                                        [
                                                            'offset'         => 89,
                                                            'messageSetSize' => 22,
                                                            'message'        =>
                                                                [
                                                                    'crc'        => 19290320,
                                                                    'magicByte'  => 0,
                                                                    'attributes' => 0,
                                                                    'key'        => '',
                                                                    'value'      => 'test1...',
                                                                ],
                                                        ],
                                                    5 =>
                                                        [
                                                            'offset'         => 90,
                                                            'messageSetSize' => 22,
                                                            'message'        =>
                                                                [
                                                                    'crc'        => 328464190,
                                                                    'magicByte'  => 0,
                                                                    'attributes' => 0,
                                                                    'key'        => '',
                                                                    'value'      => 'test2...',
                                                                ],
                                                        ],
                                                ],
                                        ],
                                ],
                        ],
                ],
            'responseHeader' =>
                [
                    'correlationId' => 1,
                ],
            'size'           => 243,
        ];
        var_dump($response->toArray());exit;
        $this->assertEquals($expected, $response->toArray());
    }

}
