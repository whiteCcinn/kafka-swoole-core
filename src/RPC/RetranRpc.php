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
 * Class RetranRpc
 *
 * @package Kafka\RPC
 */
class RetranRpc extends BaseRpc
{
    public function retran(): int
    {
        if (
            env('KAFKA_CLIENT_API_MODE') === ClientApiModeEnum::getTextByCode(ClientApiModeEnum::LOW_LEVEL)
            &&
            env('KAFKA_MESSAGE_STORAGE') === MessageStorageEnum::getTextByCode(MessageStorageEnum::REDIS)
        ) {
            /** @var Redis $redis */
            ['handler' => $redis] = RedisPool::getInstance()->get();
            $processingSize = $redis->lLen(RedisStorage::getInstance()->getProcessingKey());
            $data = $redis->lRange(RedisStorage::getInstance()->getProcessingKey(), 0, -1);
            $currentTime = time();
            foreach ($data as $item) {
                $itemArray = json_decode($item, true);
                ['time' => $time, 'message' => $message] = $itemArray;
                if (($currentTime - $time) > 300) {
                    $itemArray['time'] = time();
                    $redis->lPush(RedisStorage::getInstance()->getPendingKey(), json_encode($itemArray));
                    $redis->lRem(RedisStorage::getInstance()->getProcessingKey(), $item);
                }
            }

            RedisPool::getInstance()->put($redis);

            return $blockSize;
        }

        return 0;
    }

    public function onGetBlockSize(array $multipleInfos): int
    {
        return current($multipleInfos);
    }
}