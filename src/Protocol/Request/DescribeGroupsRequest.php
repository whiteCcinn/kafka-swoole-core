<?php
declare(strict_types=1);

namespace Kafka\Protocol\Request;

use Kafka\Protocol\AbstractRequest;

use Kafka\Protocol\Type\String16;

class DescribeGroupsRequest extends AbstractRequest
{
    /** @var String16[] $groups */
    private $groups;

    /**
     * @return String16[]
     */
    public function getGroups(): array
    {
        return $this->groups;
    }

    /**
     * @param String16[] $groups
     *
     * @return DescribeGroupsRequest
     */
    public function setGroups(array $groups): DescribeGroupsRequest
    {
        $this->groups = $groups;

        return $this;
    }
}
