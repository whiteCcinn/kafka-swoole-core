<?php
declare(strict_types=1);

namespace Kafka\RPC;

use App\App;
use Kafka\ClientKafka;
use Kafka\Enum\ClientApiModeEnum;
use Kafka\Enum\MessageStorageEnum;
use Kafka\Pool\RedisPool;
use Kafka\Storage\RedisStorage;
use Swoole\Coroutine\Redis;

/**
 * Class ClearLogRpc
 *
 * @package Kafka\RPC
 */
class ClearLogRpc extends BaseRpc
{
    public function getBlockSize(): int
    {
        if (
            env('KAFKA_CLIENT_API_MODE') === ClientApiModeEnum::getTextByCode(ClientApiModeEnum::LOW_LEVEL)
            &&
            env('KAFKA_MESSAGE_STORAGE') === MessageStorageEnum::getTextByCode(MessageStorageEnum::REDIS)
        ) {
            /** @var Redis $redis */
            ['handler' => $redis] = RedisPool::getInstance()->get();
            $blockSize = $redis->lLen(RedisStorage::getInstance()->getPendingKey());
            RedisPool::getInstance()->put($redis);

            if (!is_int($blockSize)) {
                $blockSize = 0;
            }

            return $blockSize;
        }

        return 0;
    }

    public function onGetBlockSize(array $multipleInfos): int
    {
        return current($multipleInfos);
    }
}