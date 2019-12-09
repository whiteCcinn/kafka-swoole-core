<?php
declare(strict_types=1);

namespace Kafka\Protocol\Response\Fetch;

use App\App;
use Kafka\ClientKafka;
use Kafka\Enum\CompressionCodecEnum;
use Kafka\Enum\CoroutinesEnum;
use Kafka\Enum\ProtocolTypeEnum;
use Kafka\Log\KafkaLog;
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

    /**
     * @param $protocol
     *
     * @return bool
     * @throws \Kafka\Exception\ProtocolTypeException
     * @throws \ReflectionException
     */
    public function onRecordSet(&$protocol)
    {
        $startTime = time();
        $recordSet = [];
        while (is_string($protocol) && strlen($protocol) > 0) {

            $commonResponse = new CommonResponse();
            $instance = new MessageSetFetch();
            $commonResponse->unpackProtocol(MessageSetFetch::class, $instance, $protocol);

            $buffer = $instance->getMessage()->getValue()->getValue();
            if ($buffer === null) {
                continue;
            }
            // Insufficient reading sub-section, the message is put on the next read
            if ($instance->getMessage()->getCrc()->getValue() === null) {
                continue;
            }
            // Internal decompression
            if ($instance->getMessage()->getAttributes()->getValue() !== CompressionCodecEnum::NORMAL) {
                $buffer = $instance->getMessage()->getValue()->getValue();
                if ($buffer === null) {
                    continue;
                }
                InternalDecompression:

                $instance = new MessageSetFetch();
                $commonResponse->unpackProtocol(MessageSetFetch::class, $instance, $buffer);
                // Insufficient reading sub-section, the message is put on the next read
                if ($instance->getMessage()->getCrc()->getValue() === null) {
                    continue;
                }
                $recordSet[] = $instance;
                if (!empty($buffer)) {
                    goto InternalDecompression;
                }
            } else {
                $recordSet[] = $instance;
            }

            if ((time() - $startTime) > 10) {
                KafkaLog::getInstance()
                        ->warning(sprintf('Parsed data is too slow, and producers compress too much data, partition: %s, current: %d ',
                            $this->getPartitionHeader()->getPartition()->getValue(), count($recordSet)));
            }
        }

        // sort by offset
        usort($recordSet, function (MessageSetFetch $item1, MessageSetFetch $item2) {
            if ($item1->getOffset()->getValue() === $item2->getOffset()->getValue()) {
                return 0;
            }

            return ($item1->getOffset()->getValue()) > ($item2->getOffset()->getValue()) ? 1 : -1;
        });

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
