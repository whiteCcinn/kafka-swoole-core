<?php
declare(strict_types=1);

namespace Kafka\Protocol\Response\DescribeGroups;

use Kafka\Protocol\TraitStructure\ToArrayTrait;
use Kafka\Protocol\Type\Int16;
use Kafka\Protocol\Type\String16;

class GroupsDescribeGroups
{
    use ToArrayTrait;

    /**
     * The describe error, or 0 if there was no error.
     *
     * @var Int16 $errorCode
     */
    private $errorCode;

    /**
     * 	The group ID string.
     *
     * @var String16 $groupId
     */
    private $groupId;

    /**
     * The group state string, or the empty string.
     *
     * @var String16 $groupState
     */
    private $groupState;

    /**
     * The group protocol type, or the empty string.
     *
     * @var String16 $protocolType
     */
    private $protocolType;

    /**
     * 	The group protocol data, or the empty string.
     * 
     * @var String16 $protocolData
     */
    private $protocolData;

    /**
     * @var MembersDescribeGroups[] $members
     */
    private $members;

    /**
     * @return Int16
     */
    public function getErrorCode(): Int16
    {
        return $this->errorCode;
    }

    /**
     * @param Int16 $errorCode
     *
     * @return GroupsDescribeGroups
     */
    public function setErrorCode(Int16 $errorCode): GroupsDescribeGroups
    {
        $this->errorCode = $errorCode;

        return $this;
    }

    /**
     * @return String16
     */
    public function getGroupId(): String16
    {
        return $this->groupId;
    }

    /**
     * @param String16 $groupId
     *
     * @return GroupsDescribeGroups
     */
    public function setGroupId(String16 $groupId): GroupsDescribeGroups
    {
        $this->groupId = $groupId;

        return $this;
    }

    /**
     * @return String16
     */
    public function getGroupState(): String16
    {
        return $this->groupState;
    }

    /**
     * @param String16 $groupState
     *
     * @return GroupsDescribeGroups
     */
    public function setGroupState(String16 $groupState): GroupsDescribeGroups
    {
        $this->groupState = $groupState;

        return $this;
    }

    /**
     * @return String16
     */
    public function getProtocolType(): String16
    {
        return $this->protocolType;
    }

    /**
     * @param String16 $protocolType
     *
     * @return GroupsDescribeGroups
     */
    public function setProtocolType(String16 $protocolType): GroupsDescribeGroups
    {
        $this->protocolType = $protocolType;

        return $this;
    }

    /**
     * @return String16
     */
    public function getProtocolData(): String16
    {
        return $this->protocolData;
    }

    /**
     * @param String16 $protocolData
     *
     * @return GroupsDescribeGroups
     */
    public function setProtocolData(String16 $protocolData): GroupsDescribeGroups
    {
        $this->protocolData = $protocolData;

        return $this;
    }

    /**
     * @return MembersDescribeGroups[]
     */
    public function getMembers(): array
    {
        return $this->members;
    }

    /**
     * @param MembersDescribeGroups[] $members
     *
     * @return GroupsDescribeGroups
     */
    public function setMembers(array $members): GroupsDescribeGroups
    {
        $this->members = $members;

        return $this;
    }
}
