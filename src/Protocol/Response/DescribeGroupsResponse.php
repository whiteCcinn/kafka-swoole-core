<?php
declare(strict_types=1);

namespace Kafka\Protocol\Response;

use Kafka\Protocol\AbstractResponse;
use Kafka\Protocol\Response\DescribeGroups\GroupsDescribeGroups;
use Kafka\Protocol\Response\Metadata\BrokerMetadata;
use Kafka\Protocol\Response\Metadata\TopicMetadata;
use Kafka\Protocol\TraitStructure\ToArrayTrait;

class DescribeGroupsResponse extends AbstractResponse
{
    use ToArrayTrait;

    /**
     * @var GroupsDescribeGroups[] $groups
     */
    private $groups;

    /**
     * @return GroupsDescribeGroups[]
     */
    public function getGroups(): array
    {
        return $this->groups;
    }

    /**
     * @param GroupsDescribeGroups[] $groups
     *
     * @return DescribeGroupsResponse
     */
    public function setGroups(array $groups): DescribeGroupsResponse
    {
        $this->groups = $groups;

        return $this;
    }
}
