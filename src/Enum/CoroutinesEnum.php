<?php
declare(strict_types=1);

namespace Kafka\Enum;

/**
 * Class CoroutinesEnum
 *
 * @package Kafka\Enum
 */
class CoroutinesEnum extends AbstractEnum
{
    /**
     * @message("fetch_coroutine")
     */
    public const FETCH_COROUTINE = 0;
}