<?php
declare(strict_types=1);

namespace Kafka\Storage;

use Kafka\Support\SingletonTrait;

/**
 * Class StorageAdapter
 *
 * @package Kafka\Storage
 */
class StorageAdapter implements StorageInterface
{
    use SingletonTrait;
    /**
     * @var RedisStorage | RedisStorage $adaptee
     */
    private $adaptee;

    /**
     * @return RedisStorage
     */
    public function getAdaptee(): RedisStorage
    {
        return $this->adaptee;
    }

    /**
     * @param RedisStorage $adaptee
     *
     * @return StorageAdapter
     */
    public function setAdaptee(RedisStorage $adaptee): StorageAdapter
    {
        if ($this->adaptee === null) {
            $this->adaptee = $adaptee;
        }

        return $this;
    }

    /**
     * @param array $data
     *
     * @retrun $this
     */
    public function push(array $data = [])
    {
        $result = $this->adaptee->push($data);

        return $result;
    }

    /**
     * @param int $number
     *
     * @return array
     */
    public function pop(int $number = 1)
    {
        if ($number < 1) {
            return [];
        }

        return $this->adaptee->pop($number);
    }

    /**
     * @param array $messages
     *
     * @throws \Exception
     */
    public function ack(array $messages)
    {
        $this->adaptee->ack($messages);
    }

    /**
     * @param callable|null $fn
     *
     * @return mixed
     */
    public function retran(callable $fn = null)
    {
        return $this->adaptee->retran($fn);
    }

    /**
     * @param callable|null $fn
     *
     * @return mixed
     */
    public function clear(callable $fn = null)
    {
        return $this->adaptee->clear($fn);
    }
}