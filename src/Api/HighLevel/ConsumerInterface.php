<?php

namespace Kafka\Api\HighLevel;


interface ConsumerInterface
{
    public function handler(string $topic,int $partition, int $offset, string $message);
}