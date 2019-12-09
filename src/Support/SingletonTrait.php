<?php
declare(strict_types=1);

namespace Kafka\Support;

use Kafka\Api\MetadataApi;
use Kafka\Api\OffsetCommitApi;
use Kafka\Api\OffsetFetchApi;
use Kafka\Api\ProducerApi;
use Kafka\ClientKafka;
use Kafka\Config\CommonConfig;
use Kafka\Kafka;
use Kafka\Log\KafkaLog;
use Kafka\Manager\MetadataManager;
use Kafka\Storage\RedisStorage;

trait SingletonTrait
{
    protected static $instance;

    /**
     * Need to be compatible php 7.1.x, so this scene cannot be specified return type `object`
     * @return MetadataManager | CommonConfig | Kafka | ClientKafka | ProducerApi | MetadataApi | OffsetCommitApi | KafkaLog | RedisStorage | OffsetFetchApi
     */
    public static function getInstance()
    {

        if (!isset(self::$instance[static::class]) ) {
            static::$instance[static::class] = new static();
        }

        return static::$instance[static::class];
    }

    protected function __construct()
    {
    }
}
