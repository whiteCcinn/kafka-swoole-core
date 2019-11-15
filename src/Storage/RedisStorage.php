<?php
declare(strict_types=1);

namespace Kafka\Storage;

use Kafka\Pool\RedisPool;
use Kafka\Support\SingletonTrait;
use SebastianBergmann\CodeCoverage\Report\PHP;
use Swoole\Coroutine\Redis;

/**
 * Class RedisStorage
 *
 * @package Kafka\Storage
 */
class RedisStorage
{
    use SingletonTrait;

    /**
     * @var int $configIndex
     */
    private $configIndex;

    /**
     * @var string $pendingKey
     */
    private $pendingKey;

    /**
     * @var string $processingKey
     */
    private $processingKey;

    private function init()
    {
        if ($this->configIndex === null) {
            if (preg_match('/[\s\S]*(?P<index>\d+)$/', env('KAFKA_STORAGE_REDIS'), $matches)) {
                $this->configIndex = (int)$matches['index'];
            }
            $this->pendingKey = env('KAFKA_STORAGE_REDIS_PENDING_KEY');
            $this->processingKey = env('KAFKA_STORAGE_REDIS_PROCESSING_KEY');
        }
    }

    /**
     * @param array $data
     *
     * @return RedisStorage
     * @throws \Exception
     */
    public function push(array $data = []): self
    {
        $this->init();
        /** @var Redis $redis */
        ['handler' => $redis] = RedisPool::getInstance($this->configIndex)->get($this->configIndex);
        foreach ($data as $item) {
            // The queue maximum was exceeded
            if ($redis->lLen($this->pendingKey) >= (int)env('KAFKA_STORAGE_REDIS_LIMIT', 40000)) {
                break;
            }
            $info = [
                'time'    => time(),
                'message' => $item
            ];
            $redis->lPush($this->pendingKey, json_encode($info, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE));
        }

        RedisPool::getInstance($this->configIndex)->put($redis, $this->configIndex);

        return $this;
    }

    /**
     * @param int $number
     *
     * @return array
     * @throws \Exception
     */
    public function pop(int $number = 1): array
    {
        $this->init();
        /** @var Redis $redis */
        ['handler' => $redis] = RedisPool::getInstance($this->configIndex)->get($this->configIndex);
        $messages = [];

        while (count($messages) < $number) {
            $data = $redis->rpoplpush($this->pendingKey, $this->processingKey);
            if (!empty($data)) {
                $messages[] = json_decode($data, true);
            } else {
                \co::sleep(1);
            }
        }
        RedisPool::getInstance($this->configIndex)->put($redis, $this->configIndex);

        return $messages;
    }

    /**
     * @param array $info
     *
     * @throws \Exception
     */
    public function ack(array $info)
    {
        $this->init();
        /** @var Redis $redis */
        ['handler' => $redis] = RedisPool::getInstance($this->configIndex)->get($this->configIndex);

        $redis->lRem($this->processingKey, json_encode($info, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE), 1);

        RedisPool::getInstance($this->configIndex)->put($redis, $this->configIndex);
    }

    /**
     * @return bool
     * @throws \Exception
     */
    public function retran(callable $fn = null): bool
    {
        $this->init();
        /** @var Redis $redis */
        ['handler' => $redis] = RedisPool::getInstance($this->configIndex)->get($this->configIndex);
        $number = $redis->lLen($this->processingKey);
        if ($fn !== null && is_callable($fn)) {
            $datas = $redis->lRange($this->processingKey, 0, -1);
            $lastPendingCount = $redis->lLen($this->pendingKey);
            $lastprocessingCount = $redis->lLen($this->processingKey);
            foreach ($datas as $item) {
                $ret = $fn($item);
                if ($ret) {
                    $pushRet = $redis->lPush($this->pendingKey, $item);
                    $remRet = $redis->lRem($this->processingKey, $item, 1);
                }
            }

            RedisPool::getInstance($this->configIndex)->put($redis, $this->configIndex);
            if ($pushRet > $lastCount && $remRet < $lastprocessingCount) {
                return true;
            } else {
                return false;
            }
        } else {
            while ($number > 0) {
                $data = $redis->rpoplpush($this->processingKey, $this->pendingKey);
                if (!empty($data)) {
                    $number--;
                }
            }
        }

        RedisPool::getInstance($this->configIndex)->put($redis, $this->configIndex);

        return true;
    }

    /**
     * @return string
     */
    public function getPendingKey(): string
    {
        return $this->pendingKey;
    }

    /**
     * @return string
     */
    public function getProcessingKey(): string
    {
        return $this->processingKey;
    }
}