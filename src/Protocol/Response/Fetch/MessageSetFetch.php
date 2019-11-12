<?php
declare(strict_types=1);

namespace Kafka\Protocol\Response\Fetch;

use Kafka\Enum\ProtocolTypeEnum;
use Kafka\Protocol\CommonRequest;
use Kafka\Protocol\CommonResponse;
use Kafka\Protocol\TraitStructure\ToArrayTrait;
use Kafka\Protocol\Type\Int32;
use Kafka\Protocol\Type\Int64;

class MessageSetFetch
{
    use ToArrayTrait;

    /**
     * @var Int64 $offset
     */
    private $offset;

    /**
     * @var Int32 $messageSize
     */
    private $messageSize;

    /**
     * @var MessageFetch $message
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
     * @return MessageSetFetch
     */
    public function setOffset(Int64 $offset): MessageSetFetch
    {
        $this->offset = $offset;

        return $this;
    }

    /**
     * @return Int32
     */
    public function getMessageSize(): Int32
    {
        return $this->messageSize;
    }

    /**
     * @param Int32 $messageSize
     *
     * @return MessageSetFetch
     */
    public function setMessageSize(Int32 $messageSize): MessageSetFetch
    {
        $this->messageSize = $messageSize;

        return $this;
    }

    /**
     * @return MessageFetch
     */
    public function getMessage(): MessageFetch
    {
        return $this->message;
    }

    /**
     * @param MessageFetch $message
     *
     * @return MessageSetFetch
     */
    public function setMessage(MessageFetch $message): MessageSetFetch
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
    public function onMessage(&$protocol)
    {
        if (is_string($protocol) && !empty($protocol)) {
            // Insufficient reading sub-section, the message is put on the next read
            if ($this->getMessageSize()->getValue() > strlen($protocol)) {
                $instance = new MessageFetch();
                $instance->setCrc(Int32::value(null));
                $this->setMessage($instance);
                $protocol = '';
            } else {
                $buffer = substr($protocol, 0, $this->getMessageSize()->getValue());
                $protocol = substr($protocol, $this->getMessageSize()->getValue());

                $commonResponse = new CommonResponse();
                $instance = new MessageFetch();
                $commonResponse->unpackProtocol(MessageFetch::class, $instance, $buffer);
                $this->setMessage($instance);
            }
        } else {
            $instance = new MessageFetch();
            $instance->setCrc(Int32::value(null));
            $this->setMessage($instance);
        }

        return true;
    }
}
