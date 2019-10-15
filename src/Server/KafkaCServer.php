<?php
declare(strict_types=1);

namespace Kafka\Server;

use App\Handler\HighLevelHandler;
use Co\Socket;
use http\Exception\RuntimeException;
use Kafka\Enum\ClientApiModeEnum;
use Kafka\Event\CoreLogicAfterEvent;
use Kafka\Event\CoreLogicBeforeEvent;
use Kafka\Event\CoreLogicEvent;
use Kafka\Event\SinkerEvent;
use Kafka\Event\SinkerOtherEvent;
use Swoole\Process;
use Swoole\Runtime;
use Swoole\Server;
use \co;

class KafkaCServer
{
    /**
     * @var KafkaCServer $instance
     */
    private static $instance;

    /**
     * @var Server $server
     */
    private $server;

    /**
     * @var int $masterPid
     */
    private $masterPid;

    /**
     * @var int $nextKafkaIndex
     */
    private $nextKafkaIndex = 0;

    /**
     * @var array $kafkaProcesses
     */
    private $kafkaProcesses = [];

    /**
     * @var int $nextSinkerIndex
     */
    private $nextSinkerIndex = 0;

    /**
     * @var array $sinkerProcesses
     */
    private $sinkerProcesses = [];

    /**
     * KafkaCServer constructor.
     */
    private function __construct()
    {
        swoole_set_process_name($this->getMasterName());

        $this->server = new Socket(AF_INET, SOCK_STREAM, 0);
        $this->server->bind(env('SERVER_IP'), (int)env('SERVER_PORT'));
        $this->server->listen(128);
        $this->masterPid = posix_getpid();
    }

    /**
     * @return KafkaCServer
     */
    public static function getInstance(): KafkaCServer
    {
        if (!self::$instance instanceof self) {
            self::$instance = new self();
        }

        return self::$instance;
    }

    public function start(): void
    {
        $this->registerSignal();
        go(function () {
            while (true) {
                echo "Accept: \n";
                $client = $this->server->accept();
                if ($client === false) {
                    var_dump($this->server->errCode);
                } else {
                    var_dump($client);
                }
            }
        });
    }

    private function registerSignal()
    {
        // Recycle child process
        Process::signal(SIGCHLD, [$this, 'processWait']);

        Process::signal(SIGINT, [$this, 'closeProcess']);

        Process::signal(SIGTERM, [$this, 'closeProcess']);
    }

    /**
     * @return bool
     */
    public function closeProcess(): bool
    {
        $process = array_merge($this->sinkerProcesses, $this->kafkaProcesses);
        $ret = [];
        foreach ($process as $pid) {
            if (Process::kill($pid, 0)) {
                $ret[] = Process::kill($pid, SIGTERM);
            }
        }

        if (count($ret) === count($process)) {
            exit(0);
        } else {
            exit(1);
        }

        return true;
    }

    /**
     * @return Socket
     */
    public function getServer(): Socket
    {
        return $this->server;
    }

    public function onManagerStart()
    {
        swoole_set_process_name($this->getManagerName());
    }

    public function onReceive($serv, $fd, $reactor_id, $data)
    {
        //群发收到的消息
//            $process->write($data);
        var_dump('接收到消息');
        var_dump($data);
    }

    public function setSinkerProcess(int $processNum): KafkaCServer
    {
        for ($i = 0; $i < $processNum; $i++) {
            $this->createSinkerProcess();
        }

        return self::getInstance();
    }

    public function createSinkerProcess($index = null)
    {
        if (is_null($index)) {
            $index = $this->nextSinkerIndex;
            $this->nextSinkerIndex++;
        }
        $process = new Process(function (Process $process) {
            swoole_set_process_name($this->getProcessName('sinker'));
            Runtime::enableCoroutine(true, SWOOLE_HOOK_FILE);

            go(function () {
                dispatch(new SinkerOtherEvent(), SinkerOtherEvent::NAME);
            });

            // Receiving process messages
            swoole_event_add($process->pipe, function () use ($process) {
                $msg = $process->read();
                var_dump($msg);
            });

            // Sinker Logic
            go(function () {
                dispatch(new SinkerEvent(), SinkerEvent::NAME);
            });
        }, false, 1, true);

        $pid = $process->start();
        $this->sinkerProcesses[$index] = $pid;

        return $pid;
    }

    public function setKafkaProcess(int $processNum): KafkaCServer
    {
        for ($i = 0; $i < $processNum; $i++) {
            $this->createKafkaProcess();
        }

        return self::getInstance();
    }

    public function createKafkaProcess($index = null)
    {
        if (is_null($index)) {
            $index = $this->nextKafkaIndex;
            $this->nextKafkaIndex++;
        }
        $process = new Process(function (Process $process) {
            swoole_set_process_name($this->getProcessName());

//            Process::signal(SIGINT, function () {
//                // 退出消费者组
//            });
//
//            Process::signal(SIGTERM, function () {
//                // 退出消费者组
//            });

            // Receiving process messages
            swoole_event_add($process->pipe, function () use ($process) {
                $msg = $process->read();
                var_dump($msg);
            });

            // Heartbeat
            go(function () use ($process) {
                while (true) {
                    $this->checkMasterPid($process);
                    echo sprintf('pid:%d,Check if the service master process exists every %s seconds...' . PHP_EOL,
                        getmypid(), 60);
                    co::sleep(60);
                }
            });

            // Core Logic
            go(function () {
                dispatch(new CoreLogicBeforeEvent(), CoreLogicBeforeEvent::NAME);
                dispatch(new CoreLogicEvent(), CoreLogicEvent::NAME);
                dispatch(new CoreLogicAfterEvent(), CoreLogicAfterEvent::NAME);
            });
        }, false, 1, true);

        $pid = $process->start();
        $this->kafkaProcesses[$index] = $pid;

        return $pid;
    }

    public function checkMasterPid(Process $process)
    {
        if (!Process::kill($this->masterPid, 0)) {
            $process->exit(0);
        }
    }

    /**
     * @param $ret
     *
     * @throws \Exception
     */
    public function rebootKafkaProcess($ret)
    {
        $pid = $ret['pid'];
        $index = array_search($pid, $this->kafkaProcesses);
        if ($index !== false) {
            $index = intval($index);
            $new_pid = $this->createKafkaProcess($index);
            echo "rebootKafkaProcess: {$index}={$new_pid} Done\n";

            return;
        }
        throw new \Exception('rebootKafkaProcess Error: no pid');
    }

    /**
     * @param $ret
     *
     * @throws \Exception
     */
    public function rebootSinkerProcess($ret)
    {
        $pid = $ret['pid'];
        $index = array_search($pid, $this->sinkerProcesses);
        if ($index !== false) {
            $index = intval($index);
            $new_pid = $this->createSinkerProcess($index);
            echo "rebootSinkerProcess: {$index}={$new_pid} Done\n";

            return;
        }
        throw new \Exception('rebootSinkerProcess Error: no pid');
    }

    /**
     * @throws \Exception
     */
    public function processWait($sig)
    {
        while ($ret = Process::wait(false)) {
            $pid = $ret['pid'];
            $index = array_search($pid, $this->sinkerProcesses);
            if ($index === false) {
                $index = array_search($pid, $this->kafkaProcesses);
                if ($index !== false) {
                    $this->rebootKafkaProcess($ret);
                } else {
                    throw new \RuntimeException('reboot error');
                }
            } else {
                $this->rebootSinkerProcess($ret);
            }
        }
    }

    /**
     * @param string $type
     *
     * @return string
     */
    private function getProcessName($type = 'kafka'): string
    {
        return env('APP_NAME') . ':process' . ":{$type}";
    }

    /**
     * @return string
     */
    private function getMasterName(): string
    {
        return env('APP_NAME') . ':master';
    }

    /**
     * @return string
     */
    private function getManagerName(): string
    {
        return env('APP_NAME') . ':manager';
    }
}