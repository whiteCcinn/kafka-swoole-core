<?php
declare(strict_types=1);

namespace Kafka\Enum;

/**
 * Class CompressionCodecEnum
 *
 * @package Kafka\Enum
 */
class CompressionCodecEnum extends AbstractEnum
{
    /**
     * @message("NORMAL")
     */
    public const NORMAL = 0;

    /**
     * @message("GZIP")
     */
    public const GZIP = 1;

    /**
     * @message("SNAPPY")
     */
    public const SNAPPY = 2;

    /**
     * @message("LZ4")
     */
    public const LZ4 = 3;
}