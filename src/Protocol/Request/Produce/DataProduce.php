<?php
declare(strict_types=1);

namespace Kafka\Protocol\Request\Produce;

use Kafka\Enum\CompressionCodecEnum;
use Kafka\Protocol\CommonRequest;
use Kafka\Protocol\Type\Bytes32;
use Kafka\Protocol\Type\Int32;
use Kafka\Protocol\Type\Int64;
use Kafka\Protocol\Type\Int8;

/**
 * Class DataProduce
 *
 *
 * @package Kafka\Protocol\Request\Metadata
 */
class DataProduce
{
    /**
     * Topic partition id
     *
     * @var Int32 $partition
     */
    private $partition;

    /**
     * @var MessageSetProduce[] $messageSet
     */
    private $messageSet;

    /**
     * @return Int32
     */
    public function getPartition(): Int32
    {
        return $this->partition;
    }

    /**
     * @param Int32 $partition
     *
     * @return DataProduce
     */
    public function setPartition(Int32 $partition): DataProduce
    {
        $this->partition = $partition;

        return $this;
    }

    /**
     * @return MessageSetProduce[]
     */
    public function getMessageSet(): array
    {
        return $this->messageSet;
    }

    /**
     * @param MessageSetProduce[] $messageSet
     *
     * @return DataProduce
     */
    public function setMessageSet(array $messageSet): DataProduce
    {
        $this->messageSet = $messageSet;

        return $this;
    }

    /**
     * @param $protocol
     *
     * @throws \Kafka\Exception\ProtocolTypeException
     * @throws \ReflectionException
     */
    public function onMessageSet(&$protocol)
    {
        $commentRequest = new CommonRequest();
        $data = '';

        foreach ($this->getMessageSet() as $messageSet) {
            $attributes[] = $messageSet->getMessage()->getAttributes()->getValue();
            $messageSet->getMessage()->setAttributes(Int8::value(CompressionCodecEnum::NORMAL));
            $data .= $commentRequest->packProtocol(MessageSetProduce::class, $messageSet);
        }

        if (count(array_unique($attributes)) !== 1) {
            throw new \RuntimeException('Batch and need to use compression protocol when the need for uniform compression');
        }

        $attributes = current($attributes);
        if ((($attributes & 0x07) !== CompressionCodecEnum::NORMAL)) {
            if (($attributes & 0x07) === CompressionCodecEnum::SNAPPY) {
                $compressValue = snappy_compress($data);
            } elseif (($attributes & 0x07) === CompressionCodecEnum::GZIP) {
                $compressValue = gzencode($data);
            } else {
                throw new \RuntimeException('not support lz4');
            }
        }

        $messageSet = new MessageSetProduce();
        $messageSet->setOffset(Int64::value(-1))->setMessage(
            (new MessageProduce())->setAttributes(Int8::value($attributes))
                                  ->setKey(Bytes32::value(''))
                                  ->setValue(Bytes32::value($compressValue))
        );
        $data = $commentRequest->packProtocol(MessageSetProduce::class, $messageSet);

        $protocol .= pack(Int32::getWrapperProtocol(), strlen($data)) . $data;
    }
}
