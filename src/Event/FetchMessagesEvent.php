<?php
declare(strict_types=1);

namespace Kafka\Event;

use Symfony\Contracts\EventDispatcher\Event;

class FetchMessagesEvent extends Event
{
    public const NAME = 'fetch.messages';

    private $messages;

    /**
     * FetchMessagesEvent constructor.
     *
     * @param array $messages
     */
    public function __construct(array $messages)
    {
        $this->messages = $messages;
    }

    /**
     * @return array
     */
    public function getMessages(): array
    {
        return $this->messages;
    }

    /**
     * @param array $messages
     *
     * @return FetchMessagesEvent
     */
    public function setMessages(array $messages): FetchMessagesEvent
    {
        $this->messages = $messages;

        return $this;
    }
}