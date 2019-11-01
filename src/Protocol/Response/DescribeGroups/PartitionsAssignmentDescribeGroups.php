<?php
declare(strict_types=1);

namespace Kafka\Protocol\Response\DescribeGroups;

use Kafka\Protocol\TraitStructure\ToArrayTrait;
use Kafka\Protocol\Type\Int32;
use Kafka\Protocol\Type\String16;

class PartitionsAssignmentDescribeGroups
{
    use ToArrayTrait;

    /**
     * @var String16 $topic
     */
    private $topic;

    /**
     * @var Int32[] $partitions
     */
    private $partitions;

    /**
     * @return String16
     */
    public function getTopic(): String16
    {
        return $this->topic;
    }

    /**
     * @param String16 $topic
     *
     * @return PartitionsAssignmentDescribeGroups
     */
    public function setTopic(String16 $topic): PartitionsAssignmentDescribeGroups
    {
        $this->topic = $topic;

        return $this;
    }

    /**
     * @return Int32[]
     */
    public function getPartitions(): array
    {
        return $this->partitions;
    }

    /**
     * @param Int32[] $partitions
     *
     * @return PartitionsAssignmentDescribeGroups
     */
    public function setPartitions(array $partitions): PartitionsAssignmentDescribeGroups
    {
        $this->partitions = $partitions;

        return $this;
    }
}
