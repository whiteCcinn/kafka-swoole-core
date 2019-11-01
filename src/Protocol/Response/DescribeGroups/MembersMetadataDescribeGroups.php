<?php
declare(strict_types=1);

namespace Kafka\Protocol\Response\DescribeGroups;

use Kafka\Protocol\TraitStructure\ToArrayTrait;
use Kafka\Protocol\Type\Bytes32;
use Kafka\Protocol\Type\Int16;
use Kafka\Protocol\Type\String16;

class MembersMetadataDescribeGroups
{
    use ToArrayTrait;

    /**
     * @var Int16 $version
     */
    private $version;

    /**
     * @var String16[] $topics
     */
    private $topics;

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
     * @return MembersMetadataDescribeGroups
     */
    public function setVersion(Int16 $version): MembersMetadataDescribeGroups
    {
        $this->version = $version;

        return $this;
    }

    /**
     * @return String16[]
     */
    public function getTopics(): array
    {
        return $this->topics;
    }

    /**
     * @param String16[] $topics
     *
     * @return MembersMetadataDescribeGroups
     */
    public function setTopics(array $topics): MembersMetadataDescribeGroups
    {
        $this->topics = $topics;

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
     * @return MembersMetadataDescribeGroups
     */
    public function setUserData(Bytes32 $userData): MembersMetadataDescribeGroups
    {
        $this->userData = $userData;

        return $this;
    }
}
