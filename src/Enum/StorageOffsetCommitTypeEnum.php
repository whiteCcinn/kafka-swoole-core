<?php
declare(strict_types=1);

namespace Kafka\Enum;

/**
 * Class StorageOffsetCommitTypeEnum
 *
 * @package Kafka\Enum
 */
class StorageOffsetCommitTypeEnum extends AbstractEnum
{
    /**
     * @message("AUTO")
     */
    public const AUTO = 0;

    /**
     * @message("MANUAL")
     */
    public const MANUAL = 1;
}