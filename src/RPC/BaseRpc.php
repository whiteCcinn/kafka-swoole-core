<?php
declare(strict_types=1);

namespace Kafka\RPC;

use Kafka\Api\OffsetFetchApi;
use Kafka\ClientKafka;

/**
 * Class BaseRpc
 *
 * @package Kafka\RPC
 */
class BaseRpc
{
    final static public function getNamespace(): string
    {
        return __NAMESPACE__;
    }
}