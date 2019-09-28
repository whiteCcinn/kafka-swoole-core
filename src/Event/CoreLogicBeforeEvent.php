<?php
declare(strict_types=1);

namespace Kafka\Event;

use Symfony\Contracts\EventDispatcher\Event;

class CoreLogicBeforeEvent extends Event
{
    public const NAME = 'corelogic.before';
}