<?php
declare(strict_types=1);

namespace Kafka\Event;

use Symfony\Contracts\EventDispatcher\Event;

class SetKafkaProcessNameEvent extends Event
{
    public const NAME = 'set.kafka.process.name';

    /**
     * @var int $index
     */
    private $index;

    public function __construct(int $index)
    {
        $this->index = $index;
    }

    /**
     * @return int
     */
    public function getIndex(): int
    {
        return $this->index;
    }

    /**
     * @param int $index
     *
     * @return SetKafkaProcessNameEvent
     */
    public function setIndex(int $index): SetKafkaProcessNameEvent
    {
        $this->index = $index;

        return $this;
    }
}