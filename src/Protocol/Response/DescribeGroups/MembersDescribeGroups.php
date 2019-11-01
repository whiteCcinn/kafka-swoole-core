<?php
declare(strict_types=1);

namespace Kafka\Protocol\Response\DescribeGroups;

use Kafka\Enum\ProtocolTypeEnum;
use Kafka\Protocol\CommonResponse;
use Kafka\Protocol\TraitStructure\ToArrayTrait;
use Kafka\Protocol\Type\String16;

class MembersDescribeGroups
{
    use ToArrayTrait;

    /**
     * The member ID assigned by the group coordinator.
     *
     * @var String16 $memberId
     */
    private $memberId;

    /**
     * The client ID used in the member's latest join group request.
     *
     * @var String16 $clientId
     */
    private $clientId;

    /**
     * The client host.
     *
     * @var String16 $clientHost
     */
    private $clientHost;

    /**
     * The metadata corresponding to the current group protocol in use.
     *
     * @var MembersMetadataDescribeGroups $memberMetadata
     */
    private $memberMetadata;

    /**
     * The current assignment provided by the group leader.
     *
     * @var MembersAssignmentDescribeGroups $memberAssignment
     */
    private $memberAssignment;

    /**
     * @return String16
     */
    public function getMemberId(): String16
    {
        return $this->memberId;
    }

    /**
     * @param String16 $memberId
     *
     * @return MembersDescribeGroups
     */
    public function setMemberId(String16 $memberId): MembersDescribeGroups
    {
        $this->memberId = $memberId;

        return $this;
    }

    /**
     * @return String16
     */
    public function getClientId(): String16
    {
        return $this->clientId;
    }

    /**
     * @param String16 $clientId
     *
     * @return MembersDescribeGroups
     */
    public function setClientId(String16 $clientId): MembersDescribeGroups
    {
        $this->clientId = $clientId;

        return $this;
    }

    /**
     * @return String16
     */
    public function getClientHost(): String16
    {
        return $this->clientHost;
    }

    /**
     * @param String16 $clientHost
     *
     * @return MembersDescribeGroups
     */
    public function setClientHost(String16 $clientHost): MembersDescribeGroups
    {
        $this->clientHost = $clientHost;

        return $this;
    }

    /**
     * @return MembersMetadataDescribeGroups
     */
    public function getMemberMetadata(): MembersMetadataDescribeGroups
    {
        return $this->memberMetadata;
    }

    /**
     * @param MembersMetadataDescribeGroups $memberMetadata
     *
     * @return MembersDescribeGroups
     */
    public function setMemberMetadata(MembersMetadataDescribeGroups $memberMetadata): MembersDescribeGroups
    {
        $this->memberMetadata = $memberMetadata;

        return $this;
    }

    /**
     * @return MembersAssignmentDescribeGroups
     */
    public function getMemberAssignment(): MembersAssignmentDescribeGroups
    {
        return $this->memberAssignment;
    }

    /**
     * @param MembersAssignmentDescribeGroups $memberAssignment
     *
     * @return MembersDescribeGroups
     */
    public function setMemberAssignment(MembersAssignmentDescribeGroups $memberAssignment): MembersDescribeGroups
    {
        $this->memberAssignment = $memberAssignment;

        return $this;
    }

    /**
     * @param $protocol
     *
     * @return bool
     * @throws \Kafka\Exception\ProtocolTypeException
     * @throws \ReflectionException
     */
    public function onMemberMetadata(&$protocol)
    {
        $buffer = substr($protocol, 0, ProtocolTypeEnum::B32);
        $protocol = substr($protocol, ProtocolTypeEnum::B32);
        $data = unpack(ProtocolTypeEnum::getTextByCode(ProtocolTypeEnum::B32), $buffer);
        $data = is_array($data) ? array_shift($data) : $data;

        // uint32(4294967295) is int32 (-1)
        if ($data === 4294967295) {
            $data = '';
        } else {
            $length = $data;
            $data = substr($protocol, 0, $length);
            $protocol = substr($protocol, $length);
        }

        $commonResponse = new CommonResponse();
        $instance = new MembersMetadataDescribeGroups();
        $commonResponse->unpackProtocol(MembersMetadataDescribeGroups::class, $instance, $data);
        $this->setMemberMetadata($instance);

        return true;
    }

    public function onMemberAssignment(&$protocol)
    {
        $buffer = substr($protocol, 0, ProtocolTypeEnum::B32);
        $protocol = substr($protocol, ProtocolTypeEnum::B32);
        $data = unpack(ProtocolTypeEnum::getTextByCode(ProtocolTypeEnum::B32), $buffer);
        $data = is_array($data) ? array_shift($data) : $data;

        // uint32(4294967295) is int32 (-1)
        if ($data === 4294967295) {
            $data = '';
        } else {
            $length = $data;
            $data = substr($protocol, 0, $length);
            $protocol = substr($protocol, $length);
        }

        $commonResponse = new CommonResponse();
        $instance = new MembersAssignmentDescribeGroups();
        $commonResponse->unpackProtocol(MembersAssignmentDescribeGroups::class, $instance, $data);
        $this->setMemberAssignment($instance);

        return true;
    }
}
