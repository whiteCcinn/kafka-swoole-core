<?php
declare(strict_types=1);

namespace Kafka\Enum;

/**
 * Class MessageReliabilityEnum
 *
 * @package Kafka\Enum
 */
class MessageReliabilityEnum extends AbstractEnum
{
    /**
     * @message("LOW")
     */
    public const LOW = 0;

    /**
     * @message("HIGH")
     */
    public const HIGH = 1;
}