<?php
declare(strict_types=1);

namespace Kafka\Protocol\Request\OffsetCommit;

use Kafka\Protocol\Type\Int32;
use Kafka\Protocol\Type\Int64;
use Kafka\Protocol\Type\String16;

class PartitionsOffsetCommit
{
    /**
     * The partition index.
     *
     * @var Int32 $partitionIndex
     */
    private $partitionIndex;

    /**
     * The message offset to be committed.
     *
     * @var Int64 $committedOffset
     */
    private $committedOffset;

    /**
     * 	Any associated metadata the client wants to keep.
     *
     * @var String16 $committedMetadata
     */
    private $committedMetadata;

    /**
     * @return Int32
     */
    public function getPartitionIndex(): Int32
    {
        return $this->partitionIndex;
    }

    /**
     * @param Int32 $partitionIndex
     *
     * @return PartitionsOffsetCommit
     */
    public function setPartitionIndex(Int32 $partitionIndex): PartitionsOffsetCommit
    {
        $this->partitionIndex = $partitionIndex;

        return $this;
    }

    /**
     * @return Int64
     */
    public function getCommittedOffset(): Int64
    {
        return $this->committedOffset;
    }

    /**
     * @param Int64 $committedOffset
     *
     * @return PartitionsOffsetCommit
     */
    public function setCommittedOffset(Int64 $committedOffset): PartitionsOffsetCommit
    {
        $this->committedOffset = $committedOffset;

        return $this;
    }

    /**
     * @return String16
     */
    public function getCommittedMetadata(): String16
    {
        return $this->committedMetadata;
    }

    /**
     * @param String16 $committedMetadata
     *
     * @return PartitionsOffsetCommit
     */
    public function setCommittedMetadata(String16 $committedMetadata): PartitionsOffsetCommit
    {
        $this->committedMetadata = $committedMetadata;

        return $this;
    }
}
