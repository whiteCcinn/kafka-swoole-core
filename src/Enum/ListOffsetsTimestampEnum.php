<?php
declare(strict_types=1);

namespace Kafka\Enum;

/**
 * Class ListOffsetsTimestampEnum
 *
 * @package Kafka\Enum
 */
class ListOffsetsTimestampEnum extends AbstractEnum
{
    /**
     * @message("latest")
     */
    public const LATEST = -1;

    /**
     * @message("earliest")
     */
    public const EARLIEST = -2;
}