<?php
declare(strict_types=1);

namespace Kafka\Event;

use Symfony\Contracts\EventDispatcher\Event;

class FetchMessageEvent extends Event
{
    public const NAME = 'fetch.message';

    /**
     * @var string $topic
     */
    private $topic;

    /**
     * @var int $partition
     */
    private $partition;

    /**
     * @var int $offset
     */
    private $offset;

    /**
     * @var string $message
     */
    private $message;

    /**
     * FetchMessageEvent constructor.
     *
     * @param string $topic
     * @param int    $partition
     * @param int    $offset
     * @param string $message
     */
    public function __construct(string $topic, int $partition, int $offset, string $message)
    {
        $this->topic = $topic;
        $this->partition = $partition;
        $this->offset = $offset;
        $this->message = $message;
    }

    /**
     * @return int
     */
    public function getOffset(): int
    {
        return $this->offset;
    }

    /**
     * @return string
     */
    public function getMessage(): string
    {
        return $this->message;
    }

    /**
     * @return string
     */
    public function getTopic(): string
    {
        return $this->topic;
    }

    /**
     * @return int
     */
    public function getPartition(): int
    {
        return $this->partition;
    }
}