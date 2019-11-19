<?php

namespace Kafka\Api\LowLevel;


interface ConsumerInterface
{
    public function handler(string $topic, int $partition, int $offset, string $message);
}