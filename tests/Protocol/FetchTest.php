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
        $buffer = '000002fc000000010000000100146d756c6f675f636c65616e5f32345f746573743100000001000000000000000000000000000a000002c80000000000000009000002bc61a21eac0002ffffffff000002ae82534e415050590000000001000000010000029a9d1b000009012005000002ad201b8bb6090f980000029f7b2268656164657273223a7b226170705f6964223a32342c226c6f675f6e616d65223a050b28726f6c655f63686174227d051c09301463636f756e7411286831353732353837363637303032303037343634222c226167656e74055708312c2201420147286e6e656c223a32303030300d15746d7367223a22e58886e5bc80e68993efbc8ce7b2bee88bb1222c22636c6905434470223a223138322e3234302e33322e313337191d2476657273696f6e223a2201141c72656174655f746901bf11962438362c2266616d696c79098d1c303030393030303215160deed0e4b99de5b79ee5a082222c226672657175656e6379223a312c2269735f696e7465726e616c223a302c226c6173745f6f66666c696e1d6d2c333138303834372c226c657601e5303339332c226d73675f747970650548006d159924333138323332352c227021770431350d11003621182c30312c22706c6174666f726d05372583052801bb010508363839214005190dbe68e6baaae6b0b4e588abe999a2e6b091e5aebf222c22736572766572053c0039150e0c6f70656e2e230108373336014d0d2c325001187461726765745f3aef0104222c111919a700301514059501ae0113646f74616c5f7061795f676f6c64223a3132303834302c2275706605da0876696121db207c31222c227669705f2d3a08367d7d4da90c06000002feb902feb902feb902feb902feb902feb902feb902feb902feb902feb902d2b9020007feb902feb902feb902feb902feb902feb902feb902feb902feb902feb902deb9020008feb902feb902feb902feb902feb902feb902feb902feb902feb902feb902deb9020009feb902feb902feb902feb902feb902feb902feb902feb902feb902feb902c2b902';

        $response = $protocol->response;
        $response->unpack(hex2bin($buffer));

        var_dump($response->toArray());exit;
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
        $this->assertEquals($expected, $response->toArray());
    }

//    /**
//     * @author  caiwenhui
//     * @group   decode
//     * @throws \Kafka\Exception\ProtocolTypeException
//     * @throws \ReflectionException
//     */
//    public function testDecodeSnappy()
//    {
//        /** @var FetchRequest $protocol */
//        $protocol = $this->protocol;
//        $buffer = '000007f70000000100000001000d6d756c6f675f636c65616e5f30000000010000000000000000000000000010000007ca00000000000000000000009d34540d590002ffffffff0000008f82534e415050590000000001000000010000007b91010000190110856d9040a4050f00080506a00000040000006f7b2268656164657273223a7b226170705f6964223a302c226c6f675f6e616d65223a050b1474657374227d0517092a0c67656e74112c4c6964223a3232322c226e756d223a3333332c22700114483131312c227365727665725f6964223a307d7d000000000000000600000102d747d97e0002ffffffff000000f482534e41505059000000000100000001000000e0e606000009012001000000856c08264c050f00080506a00000050000006f7b2268656164657273223a7b226170705f6964223a302c226c6f675f6e616d65223a050b1474657374227d0517092a0c67656e74112c01333c3232322c226e756d223a3333332c22700114283131312c22736572766572092b047d7d0d7b000201910c6f38eb9c05101191040700fe9100e29100000301910c6732ec443291000199fe2201da2201000401910c6ea08d743291000006fe2201e62201412e108566aa8aac3291000009fe9100e69100210c10856959703c32910021c9feb301beb3010000000000000007000000a10b10838d0002ffffffff0000009382534e415050590000000001000000010000007f91010000090120070000008564022194050f00080506a000000a0000006f7b2268656164657273223a7b226170705f6964223a302c226c6f675f6e616d65223a050b1474657374227d0517092a0c67656e74112c4c6964223a3232322c226e756d223a3333332c22700114483131312c227365727665725f6964223a307d7d00000000000000080000009f6e7744ff0002ffffffff0000009182534e415050590000000001000000010000007d910100000901200800000085659a477c050f010e0101980b0000006f7b2268656164657273223a7b226170705f6964223a302c226c6f675f6e616d65223a050b1474657374227d0517092a0c67656e74112c4c6964223a3232322c226e756d223a3333332c22700114483131312c227365727665725f6964223a307d7d0000000000000009000000a176bbdfc00002ffffffff0000009382534e415050590000000001000000010000007f910100000901200900000085615377e4050f00080506a000000c0000006f7b2268656164657273223a7b226170705f6964223a302c226c6f675f6e616d65223a050b1474657374227d0517092a0c67656e74112c4c6964223a3232322c226e756d223a3333332c22700114483131312c227365727665725f6964223a307d7d000000000000000a000000a1faae6b020002ffffffff0000009382534e415050590000000001000000010000007f910100000901200a0000008560cb110c050f00080506a000000d0000006f7b2268656164657273223a7b226170705f6964223a302c226c6f675f6e616d65223a050b1474657374227d0517092a0c67656e74112c4c6964223a3232322c226e756d223a3333332c22700114483131312c227365727665725f6964223a307d7d000000000000000b000000a149e03f2d0002ffffffff0000009382534e415050590000000001000000010000007f910100000901200b000000856263ba34050f00080506a000000e0000006f7b2268656164657273223a7b226170705f6964223a302c226c6f675f6e616d65223a050b1474657374227d0517092a0c67656e74112c4c6964223a3232322c226e756d223a3333332c22700114483131312c227365727665725f6964223a307d7d000000000000000c000000a1ec5d3a280002ffffffff0000009382534e415050590000000001000000010000007f910100000901200c0000008563fbdcdc050f00080506a000000f0000006f7b2268656164657273223a7b226170705f6964223a302c226c6f675f6e616d65223a050b1474657374227d0517092a0c67656e74112c4c6964223a3232322c226e756d223a3333332c22700114483131312c227365727665725f6964223a307d7d000000000000000d000000a1b3577ec80002ffffffff0000009382534e415050590000000001000000010000007f910100000901200d000000857277b584050f00080506a00000100000006f7b2268656164657273223a7b226170705f6964223a302c226c6f675f6e616d65223a050b1474657374227d0517092a0c67656e74112c4c6964223a3232322c226e756d223a3333332c22700114483131312c227365727665725f6964223a307d7d000000000000000e000000a13f42ca0a0002ffffffff0000009382534e415050590000000001000000010000007f910100000901200e0000008573efd36c050f00080506a00000110000006f7b2268656164657273223a7b226170705f6964223a302c226c6f675f6e616d65223a050b1474657374227d0517092a0c67656e74112c4c6964223a3232322c226e756d223a3333332c22700114483131312c227365727665725f6964223a307d7d000000000000000f000000a18c0c9e250002ffffffff0000009382534e415050590000000001000000010000007f910100000901200f0000008571477854050f00080506a00000120000006f7b2268656164657273223a7b226170705f6964223a302c226c6f675f6e616d65223a050b1474657374227d0517092a0c67656e74112c4c6964223a3232322c226e756d223a3333332c22700114483131312c227365727665725f6964223a307d7d';
//
//        $response = $protocol->response;
//        $response->unpack(hex2bin($buffer));
//        var_dump($response->toArray());exit;
//    }
}
