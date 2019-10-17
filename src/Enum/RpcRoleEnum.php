<?php
declare(strict_types=1);

namespace Kafka\Enum;

/**
 * Class RpcRoleEnum
 *
 * @package Kafka\Enum
 */
class RpcRoleEnum extends AbstractEnum
{
    /**
     * @message("INTERNAL")
     */
    public const EXTERNAL = 0;

    /**
     * @message("INTERNAL")
     */
    public const INTERNAL = 1;
}