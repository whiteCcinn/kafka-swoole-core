<?php
declare(strict_types=1);

namespace Kafka\Protocol\Response\Fetch;

use Kafka\Enum\CompressionCodecEnum;
use Kafka\Enum\ProtocolTypeEnum;
use Kafka\Protocol\CommonResponse;
use Kafka\Protocol\TraitStructure\ToArrayTrait;
use Kafka\Protocol\Type\Int32;

class PartitionResponsesFetch
{
    use ToArrayTrait;

    /**
     * @var PartitionHeaderFetch $partitionHeader
     */
    private $partitionHeader;

    /**
     * @var Int32 $messageSetSize
     */
    private $messageSetSize;

    /**
     * @var MessageSetFetch[] $recordSet
     */
    private $recordSet;

    /**
     * @return PartitionHeaderFetch
     */
    public function getPartitionHeader(): PartitionHeaderFetch
    {
        return $this->partitionHeader;
    }

    /**
     * @param PartitionHeaderFetch $partitionHeader
     *
     * @return PartitionResponsesFetch
     */
    public function setPartitionHeader(PartitionHeaderFetch $partitionHeader): PartitionResponsesFetch
    {
        $this->partitionHeader = $partitionHeader;

        return $this;
    }

    /**
     * @return MessageSetFetch[]
     */
    public function getRecordSet(): array
    {
        return $this->recordSet;
    }

    /**
     * @param MessageSetFetch[] $recordSet
     *
     * @return PartitionResponsesFetch
     */
    public function setRecordSet(array $recordSet): PartitionResponsesFetch
    {
        $this->recordSet = $recordSet;

        return $this;
    }

    public function onRecordSet(&$protocol)
    {
        $recordSet = [];
        while (is_string($protocol) && strlen($protocol) > 0) {
            $commonResponse = new CommonResponse();
            $instance = new MessageSetFetch();
            $commonResponse->unpackProtocol(MessageSetFetch::class, $instance, $protocol);
            // Internal decompression
            if ($instance->getMessage()->getAttributes() !== CompressionCodecEnum::NORMAL) {
                $buffer = $instance->getMessage()->getValue()->getValue();
                $commonResponse->unpackProtocol(MessageSetFetch::class, $instance, $buffer);
            }
            $recordSet[] = $instance;
        }
        $this->setRecordSet($recordSet);

        return true;
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
     * @return PartitionResponsesFetch
     */
    public function setMessageSetSize(Int32 $messageSetSize): PartitionResponsesFetch
    {
        $this->messageSetSize = $messageSetSize;

        return $this;
    }
}
