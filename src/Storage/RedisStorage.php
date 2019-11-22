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
     * @return array
     * @throws \Exception
     */
    public function push(array $data = []): array
    {
        $this->init();
        /** @var Redis $redis */
        ['handler' => $redis] = RedisPool::getInstance($this->configIndex, true)->get($this->configIndex, true);
        $result = [];
        CheckListLen:
        $redis->lLen($this->pendingKey);
        if ($redis->recv() >= (int)env('KAFKA_STORAGE_REDIS_LIMIT', 40000)) {
            \co::sleep(1);
            goto CheckListLen;
        }
        foreach ($data as $item) {
            // The queue maximum was exceeded
            $info = [
                'time'    => time(),
                'message' => $item
            ];
            $redis->lPush($this->pendingKey, json_encode($info, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE));
        }

        for ($i = 0; $i < count($data); $i++) {
            $result[] = $redis->recv();
        }

        RedisPool::getInstance($this->configIndex, true)->put($redis, $this->configIndex, true);

        return $result;
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
        ['handler' => $redis] = RedisPool::getInstance($this->configIndex, true)->get($this->configIndex, true);
        $messages = [];
        $originNum = $number;

        $startTime = time();
        $needReturn = false;
        Rpoplpush:
        $i = 0;
        while ($i < $number) {
            if ((time() - $startTime > 60) && !empty($messages)) {
                $needReturn = true;
                goto Recv;
            }
            $redis->rpoplpush($this->pendingKey, $this->processingKey);
            $i++;
        }

        Recv:
        while ($i > 0) {
            $data = $redis->recv();
            if (!empty($data)) {
                $messages[] = json_decode($data, true);
            }
            $i--;
        }
        if (($lastCount = $originNum - count($messages)) > 0 && $needReturn === false) {
            $number = $lastCount;
            if (count($messages) === 0) {
                \co::sleep(1);
            }
            goto Rpoplpush;
        }

        RedisPool::getInstance($this->configIndex, true)->put($redis, $this->configIndex, true);

        return $messages;
    }

    /**
     * @param array $messages
     *
     * @throws \Exception
     */
    public function ack(array $messages)
    {
        $this->init();
        /** @var Redis $redis */
        ['handler' => $redis] = RedisPool::getInstance($this->configIndex, true)->get($this->configIndex, true);

        $i = 0;
        foreach ($messages as $message) {
            $redis->lRem($this->processingKey, json_encode($message, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE),
                1);
            $i++;
        }

        $result = [];
        while ($i > 0) {
            $result[] = $redis->recv();
            $i--;
        }

        RedisPool::getInstance($this->configIndex, true)->put($redis, $this->configIndex, true);
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
            if ($pushRet > $lastPendingCount && $remRet < $lastprocessingCount) {
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
     * @return bool
     * @throws \Exception
     */
    public function clear(callable $fn = null): bool
    {
        $this->init();
        /** @var Redis $redis */
        ['handler' => $redis] = RedisPool::getInstance($this->configIndex)->get($this->configIndex);
        $number = $redis->lLen($this->processingKey);
        if ($fn !== null && is_callable($fn)) {
            $datas = $redis->lRange($this->processingKey, 0, -1);
            $lastprocessingCount = $redis->lLen($this->processingKey);
            foreach ($datas as $item) {
                $ret = $fn($item);
                if ($ret) {
                    $remRet = $redis->lRem($this->processingKey, $item, 1);
                }
            }

            RedisPool::getInstance($this->configIndex)->put($redis, $this->configIndex);
            if ($remRet < $lastprocessingCount) {
                return true;
            } else {
                return false;
            }
        } else {
            $data = $redis->del($this->processingKey);
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