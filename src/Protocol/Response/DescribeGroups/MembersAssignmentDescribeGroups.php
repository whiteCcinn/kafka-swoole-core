<?php
declare(strict_types=1);

namespace Kafka\Protocol\Response\DescribeGroups;

use Kafka\Protocol\TraitStructure\ToArrayTrait;
use Kafka\Protocol\Type\Bytes32;
use Kafka\Protocol\Type\Int16;
use Kafka\Protocol\Type\String16;

class MembersAssignmentDescribeGroups
{
    use ToArrayTrait;

    /**
     * @var Int16 $version
     */
    private $version;

    /**
     * @var PartitionsAssignmentDescribeGroups[] $partitionsAssignment
     */
    private $partitionsAssignment;

    /**
     * @var Bytes32 $userData
     */
    private $userData;

    /**
     * @return Int16
     */
    public function getVersion(): Int16
    {
        return $this->version;
    }

    /**
     * @param Int16 $version
     *
     * @return MembersAssignmentDescribeGroups
     */
    public function setVersion(Int16 $version): MembersAssignmentDescribeGroups
    {
        $this->version = $version;

        return $this;
    }

    /**
     * @return PartitionsAssignmentDescribeGroups[]
     */
    public function getPartitionsAssignment(): array
    {
        return $this->partitionsAssignment;
    }

    /**
     * @param PartitionsAssignmentDescribeGroups[] $partitionsAssignment
     *
     * @return MembersAssignmentDescribeGroups
     */
    public function setPartitionsAssignment(array $partitionsAssignment): MembersAssignmentDescribeGroups
    {
        $this->partitionsAssignment = $partitionsAssignment;

        return $this;
    }

    /**
     * @return Bytes32
     */
    public function getUserData(): Bytes32
    {
        return $this->userData;
    }

    /**
     * @param Bytes32 $userData
     *
     * @return MembersAssignmentDescribeGroups
     */
    public function setUserData(Bytes32 $userData): MembersAssignmentDescribeGroups
    {
        $this->userData = $userData;

        return $this;
    }
}
