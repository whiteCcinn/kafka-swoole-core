<?php

declare(strict_types=1);

namespace Kafka\Log;

use Kafka\Support\SingletonTrait;
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

    /**
     * @param string $msg
     * @param array  $context
     * @param array  $extra
     * @param int    $level
     */
    public function write(string $msg, array $context = [], array $extra = [], int $level = Logger:: INFO)
    {
        if (!$this->init) {
            $this->init();
        }
        
        $this->logger->log($msg, $context, $extra, $level);
    }

    private function init()
    {
        $this->initLogs();

        $dateFormat = "Y-m-d\TH:i:sP";
        $output = "[%datetime%] %channel%.%level_name%: %message% %context% %extra%\n";
        $formatter = new LineFormatter($output, $dateFormat);

        $this->logger = new Logger(env('APP_NAME'));

        foreach ($this->logs as $log) {
            $stream = new StreamHandler(env('KAFKA_LOG_DIR') . '/' . $log['path'], $log['level']);
            $stream->setFormatter($formatter);
            $this->logger->pushHandler($stream);
        }

        $this->init = true;
    }

    private function initLogs()
    {
        $prefix = env('APP_NAME') . env('APP_ID', '');

        return [
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
}