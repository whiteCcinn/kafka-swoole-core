<?php
declare(strict_types=1);

namespace Kafka\Pool;

use Kafka\Log\KafkaLog;
use \Swoole\Coroutine\Channel;
use \Swoole\Coroutine\Redis;
use \RuntimeException;

/**
 * Class RedisPool
 *
 * @package Kafka\Pool
 */
class RedisPool
{

    /**
     * @var array Channel
     */
    protected $pool;

    /**
     * @var array $once
     */
    private $once;

    /**
     * @var array
     */
    protected static $instance;

    /**
     * @var int $number
     */
    private static $number;

    /**
     * RedisPool constructor.
     */
    private function __construct()
    {
    }

    /**
     * @param int  $index
     * @param bool $pipe
     *
     * @return RedisPool
     * @throws \Exception
     */
    public static function getInstance(int $index = 0, bool $pipe = false): self
    {
        if (self::$number === null) {
            self::$number = env('POOL_REDIS_NUM');
        }

        if ($index >= self::$number) {
            throw new \Exception('invalid index');
        }

        if (!isset(self::$instance[$index . ':' . (int)$pipe])) {
            static::$instance[$index . ':' . (int)$pipe] = new static();
            static::$instance[$index . ':' . (int)$pipe]->init($index, $pipe);
        }

        return static::$instance[$index . ':' . (int)$pipe];
    }


    /**
     * @param int $index
     *
     * @return RedisPool
     */
    protected function init(int $index = 0, bool $pipe = false): self
    {
        if (!isset($this->once[$index])) {
            $this->once[$index . ':' . (int)$pipe] = false;
        }
        if (!$this->once[$index . ':' . (int)$pipe]) {
            $size = (int)env("POOL_REDIS_{$index}_MAX_NUMBER");
            $maxIdle = (int)env("POOL_REDIS_{$index}_MAX_IDLE");
            $this->pool[$index . ':' . (int)$pipe] = new Channel($size);
            for ($i = 0; $i < $maxIdle; $i++) {
                $this->createConnect($index, $pipe);
            }
        }

        return $this;
    }

    /**
     * @param int $index
     */
    function createConnect(int $index, bool $pipe = false)
    {
        $host = env("POOL_REDIS_{$index}_HOST");
        $port = (int)env("POOL_REDIS_{$index}_PORT");
        $auth = env("POOL_REDIS_{$index}_AUTH");
        $db = (int)env("POOL_REDIS_{$index}_DB");
        $redis = new Redis();
        $res = $redis->connect($host, $port);
        if ($res == false) {
            KafkaLog::getInstance()->error("failed to connect redis server.{$host}:{$port}");
            throw new RuntimeException("failed to connect redis server.{$host}:{$port}");
        } else {
            if ($auth !== null) {
                $redis->auth($auth);
            }
            if ($db !== null) {
                $redis->select($db);
            }
            if ($pipe) {
                $redis->setDefer();
            }
            $this->put($redis, $index, $pipe);
        }
    }

    /**
     * @param     $redis
     * @param int $index
     *
     * @return RedisPool
     */
    function put($redis, int $index = 0, bool $pipe = false): self
    {
        if (!isset($this->once[$index . ':' . (int)$pipe])) {
            throw new RuntimeException("The pool invalid");
        }
        $this->pool[$index . ':' . (int)$pipe]->push($redis);

        return $this;
    }

    /**
     * @param int $index
     *
     * @return array
     */
    function get(int $index = 0, bool $pipe = false): array
    {
        if (!isset($this->once[$index . ':' . (int)$pipe])) {
            throw new RuntimeException("The pool invalid");
        }

        /**
         * @var Redis $redis
         */
        pop:
        $redis = $this->pool[$index . ':' . (int)$pipe]->pop();
        try {
            if ($pipe) {
                $redis->ping();
                if ($redis->recv() !== 0) {
                    $this->createConnect($index, $pipe);
                    goto pop;
                }
            } else {
                if ($redis->ping() !== 0) {
                    $this->createConnect($index, $pipe);
                    goto pop;
                }
            }
        } catch (\Exception $e) {
            KafkaLog::getInstance()->warning('Redis pool has exception' . $e->getMessage());
            $this->createConnect($index, $pipe);
            goto pop;
        }

        return ['index' => $index, 'handler' => $redis];
    }
}