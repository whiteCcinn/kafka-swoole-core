<?php

declare(strict_types=1);

namespace Kafka\Log;

use Kafka\Enum\LogLevelEnum;
use Kafka\Support\SingletonTrait;
use Monolog\Formatter\LineFormatter;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

class KafkaLog
{
    use SingletonTrait;

    private $logs = [];

    private $init = false;

    /**
     * @var Logger
     */
    private $logger;

    private function init()
    {
        $this->initLogs();

        $dateFormat = "Y-m-d\TH:i:sP";
        $output = "[%datetime%] %channel%.%level_name%: %message% %context% %extra%\n";
        $formatter = new LineFormatter($output, $dateFormat);

        $this->logger = new Logger(env('APP_NAME'));
        $level = env('KAFKA_LOG_LEVEL');
        // If debug mode, output in the terminal
        if($level !== LogLevelEnum::getTextByCode(LogLevelEnum::DEBUG)) {
            foreach ($this->logs as $log) {
                $stream = new StreamHandler(env('KAFKA_LOG_DIR') . '/' . $log['path'], $log['level']);
                $stream->setFormatter($formatter);
                $this->logger->pushHandler($stream);
            }
        }

        $this->init = true;
    }

    private function initLogs()
    {
        $prefix = env('APP_NAME') . env('APP_ID', '');

        $this->logs = [
            [
                'level' => Logger:: INFO,
                'path'  => $prefix . '.info.log'
            ],
            [
                'level' => Logger:: ERROR,
                'path'  => $prefix . '.error.log'
            ],
            [
                'level' => Logger:: NOTICE,
                'path'  => $prefix . '.exception.log'
            ],
        ];
    }

    /**
     * @param int    $level
     * @param string $msg
     * @param array  $context
     *
     * @return bool
     */
    public function log(int $level = Logger:: INFO, string $msg = '', array $context = [])
    {
        if (!$this->init) {
            $this->init();
        }

        return $this->logger->log($level, $msg, $context);
    }

    /**
     * @param string $message
     * @param array  $context
     *
     * @return bool
     */
    public function debug(string $message, array $context = [])
    {
        if (!$this->init) {
            $this->init();
        }

        return $this->logger->debug($message, $context);
    }

    /**
     * @param string $message
     * @param array  $context
     *
     * @return bool
     */
    public function info(string $message, array $context = [])
    {
        if (!$this->init) {
            $this->init();
        }

        return $this->logger->info($message, $context);
    }

    /**
     * @param string $message
     * @param array  $context
     *
     * @return bool
     */
    public function notice(string $message, array $context = [])
    {
        if (!$this->init) {
            $this->init();
        }

        return $this->logger->notice($message, $context);
    }

    /**
     * @param string $message
     * @param array  $context
     *
     * @return bool
     */
    public function warn(string $message, array $context = [])
    {
        if (!$this->init) {
            $this->init();
        }

        return $this->logger->warn($message, $context);
    }

    /**
     * @param string $message
     * @param array  $context
     *
     * @return bool
     */
    public function warning(string $message, array $context = [])
    {
        if (!$this->init) {
            $this->init();
        }

        return $this->logger->warn($message, $context);
    }

    /**
     * @param string $message
     * @param array  $context
     *
     * @return bool
     */
    public function err(string $message, array $context = [])
    {
        if (!$this->init) {
            $this->init();
        }

        return $this->logger->err($message, $context);
    }

    /**
     * @param string $message
     * @param array  $context
     *
     * @return bool
     */
    public function error(string $message, array $context = [])
    {
        if (!$this->init) {
            $this->init();
        }

        return $this->logger->error($message, $context);
    }

    /**
     * @param string $message
     * @param array  $context
     *
     * @return bool
     */
    public function crit(string $message, array $context = [])
    {
        if (!$this->init) {
            $this->init();
        }

        return $this->logger->crit($message, $context);
    }

    /**
     * @param string $message
     * @param array  $context
     *
     * @return bool
     */
    public function critical(string $message, array $context = [])
    {
        if (!$this->init) {
            $this->init();
        }

        return $this->logger->critical($message, $context);
    }

    /**
     * @param string $message
     * @param array  $context
     *
     * @return bool
     */
    public function alert(string $message, array $context = [])
    {
        if (!$this->init) {
            $this->init();
        }

        return $this->logger->alert($message, $context);
    }

    /**
     * @param string $message
     * @param array  $context
     *
     * @return bool
     */
    public function emerg(string $message, array $context = [])
    {
        if (!$this->init) {
            $this->init();
        }

        return $this->logger->emerg($message, $context);
    }

    /**
     * @param string $message
     * @param array  $context
     *
     * @return bool
     */
    public function emergency(string $message, array $context = [])
    {
        if (!$this->init) {
            $this->init();
        }

        return $this->logger->emergency($message, $context);
    }
}