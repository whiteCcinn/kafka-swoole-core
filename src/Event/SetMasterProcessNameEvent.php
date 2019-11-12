<?php
declare(strict_types=1);

namespace Kafka\Event;

use Symfony\Contracts\EventDispatcher\Event;

class SetMasterProcessNameEvent extends Event
{
    public const NAME = 'set.master.process.name';
}