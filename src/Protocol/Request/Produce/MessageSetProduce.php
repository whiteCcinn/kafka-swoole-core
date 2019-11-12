<?php
declare(strict_types=1);

namespace Kafka\Protocol\Request\Produce;

use http\Exception\RuntimeException;
use Kafka\Enum\CompressionCodecEnum;
use Kafka\Protocol\CommonRequest;
use Kafka\Protocol\Type\Bytes32;
use Kafka\Protocol\Type\Int32;
use Kafka\Protocol\Type\Int64;
use Kafka\Protocol\Type\Int8;

class MessageSetProduce
{
    /**
     * @var Int64 $offset
     */
    private $offset;

    /**
     * @var Int32 $messageSetSize
     */
    private $messageSetSize;

    /**
     * @var MessageProduce $message
     */
    private $message;

    /**
     * @return Int64
     */
    public function getOffset(): Int64
    {
        return $this->offset;
    }

    /**
     * @param Int64 $offset
     *
     * @return MessageSetProduce
     */
    public function setOffset(Int64 $offset): MessageSetProduce
    {
        $this->offset = $offset;

        return $this;
    }

    /**
     * @return Int32
     */
    public function getMessageSetSize(): Int32
    {
        return $this->messageSetSize;
    }

    /**
     * @param Int32 $messageSetSize
     *
     * @return MessageSetProduce
     */
    public function setMessageSetSize(Int32 $messageSetSize): MessageSetProduce
    {
        $this->messageSetSize = $messageSetSize;

        return $this;
    }

    /**
     * @return MessageProduce
     */
    public function getMessage(): MessageProduce
    {
        return $this->message;
    }

    /**
     * @param MessageProduce $message
     *
     * @return MessageSetProduce
     */
    public function setMessage(MessageProduce $message): MessageSetProduce
    {
        $this->message = $message;

        return $this;
    }

    /**
     * @param $protocol
     *
     * @throws \Kafka\Exception\ProtocolTypeException
     * @throws \ReflectionException
     */
    public function onMessageSetSize(&$protocol)
    {
        $this->getMessage()->setValue($this->getMessage()
                                           ->getValue());
        $commentRequest = new CommonRequest();
        $data = $commentRequest->packProtocol(MessageProduce::class, $this->getMessage());
        $data = pack(Int32::getWrapperProtocol(), strlen($data)) . $data;

        $left = 0xffffffff00000000;
        $right = 0x00000000ffffffff;
        $l = (-1 & $left) >> 32;
        $r = -1 & $right;
        $data = pack(Int64::getWrapperProtocol(), $l, $r) . $data;

        $commentRequest = new CommonRequest();
        $data = $commentRequest->packProtocol(MessageProduce::class, $this->getMessage());
        $this->setMessageSetSize(Int32::value(strlen($data)));
        $protocol .= pack(Int32::getWrapperProtocol(), $this->getMessageSetSize()->getValue());
    }
}
