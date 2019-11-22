<?php
declare(strict_types=1);

namespace Kafka\Server;

use App\App;
use App\Handler\HighLevelHandler;
use Co\Socket;
use Kafka\Api\LeaveGroupApi;
use Kafka\ClientKafka;
use Kafka\Enum\RpcRoleEnum;
use Kafka\Event\CoreLogicAfterEvent;
use Kafka\Event\CoreLogicBeforeEvent;
use Kafka\Event\CoreLogicEvent;
use Kafka\Event\ProcessExitEvent;
use Kafka\Event\SetKafkaProcessNameEvent;
use Kafka\Event\SetMasterProcessNameEvent;
use Kafka\Event\SetSinkerProcessNameEvent;
use Kafka\Event\SinkerEvent;
use Kafka\Event\SinkerOtherEvent;
use Kafka\Log\KafkaLog;
use Kafka\RPC\BaseRpc;
use Kafka\Support\Str;
use Swoole\Process;
use Swoole\Runtime;
use Swoole\Server;
use \co;
use Symfony\Component\Console\Input\ArgvInput;
use Symfony\Component\Console\Output\ConsoleOutput;
use Symfony\Component\Console\Style\SymfonyStyle;

class KafkaCServer
{
    private const  MASTER_UNIX_FILE = 'master.sock';
    private const  CHILD_UNIX_FILE  = 'child_%s.sock';

    /**
     * @var KafkaCServer $instance
     */
    private static $instance;

    /**
     * @var Socket $server
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
        dispatch(new SetMasterProcessNameEvent(), SetMasterProcessNameEvent::NAME);
        $this->createMasterUnixFile();
    }


    private function createMasterUnixFile()
    {
        if (!file_exists(self::getMatserSockFile())) {
            $this->server = new Socket(AF_UNIX, SOCK_STREAM, 0);
            $this->server->bind(self::getMatserSockFile());
            $this->server->listen(128);
            $this->masterPid = posix_getpid();
        }
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
        $this->createMasterUnixFileAccpet();
    }

    private function createMasterUnixFileAccpet()
    {
        go(function () {
            while (true) {
                $client = $this->server->accept();
                if ($client === false) {
                    \co::sleep(1);
                } else {
                    $msg = $this->getAfUnixMessage($client);
                    $msg = json_decode($msg, true);
                    ['role' => $external, 'rpc' => $rpc, 'method' => $method] = $msg;
                    if (
                        in_array(RpcRoleEnum::getTextByCode($external), RpcRoleEnum::getAllText())
                        &&
                        $external === RpcRoleEnum::EXTERNAL
                    ) {
                        foreach ($this->kafkaProcesses as $index => $item) {
                            $path = $this->getChildSockFile($index);
                            if (!file_exists($path)) {
                                $ret[$index] = [];
                                continue;
                            }
                            $socket = new Socket(AF_UNIX, SOCK_STREAM, 0);
                            $cmd = [
                                'role'   => $external,
                                'rpc'    => $rpc,
                                'method' => $method,
                            ];
                            if (isset($msg['params'])) {
                                $cmd['params'] = $msg['params'];
                            }
                            $data = json_encode($cmd);
                            $package = pack('N', strlen($data)) . $data;
                            if ($socket->connect($path)) {
                                $socket->send($package);
                                $msg = $this->getAfUnixMessage($socket);
                                $ret[$index] = json_decode($msg, true);
                                $socket->close();
                            } else {
                                $ret[$index] = [];
                            }
                        }
                        $result = call_user_func([(new $rpc), Str::camel('on_' . $method)], $ret);
                        $path = self::getMatserSockFile();
                        $data = json_encode($result);
                        $package = pack('N', strlen($data)) . $data;
                        $client->send($package);
                        $client->close();
                    }
                }
            }
        });
    }

    private function getAfUnixMessage($client): string
    {
        $len = $client->recv(4, 3);
        $len = unpack('N', $len);
        $len = is_array($len) ? current($len) : $len;
        $msg = $client->recv($len, 3);

        return $msg;
    }

    public static function getMatserSockFile(): string
    {
        $dir = env('SERVER_AF_UNIX_DIR');
        if (!Str::endsWith($dir, '/')) {
            $dir .= '/';
        }

        $path = $dir . sprintf(self::MASTER_UNIX_FILE);

        return $path;
    }

    private function getChildSockFile($index): string
    {
        $dir = env('SERVER_AF_UNIX_DIR');
        if (!Str::endsWith($dir, '/')) {
            $dir .= '/';
        }

        $path = $dir . sprintf(self::CHILD_UNIX_FILE, $index);

        return $path;
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
        $io = new SymfonyStyle(new ArgvInput(), new ConsoleOutput());
        $process = array_merge($this->sinkerProcesses, $this->kafkaProcesses);
        $ret = [];
        foreach ($process as $pid) {
            if (Process::kill($pid, 0)) {
                $ret[] = Process::kill($pid, SIGTERM);
            }
        }

        if (count($ret) === count($process)) {
            $io->success('[-] Server stoped');
            exit(0);
        } else {
            $io->error('[-] Server stoped...');
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
        $process = new Process(function (Process $process) use($index) {
            swoole_set_process_name($this->getProcessName('sinker'));
            dispatch(new SetSinkerProcessNameEvent($index), SetSinkerProcessNameEvent::NAME);
            Runtime::enableCoroutine(true, SWOOLE_HOOK_ALL);

            go(function () {
                dispatch(new SinkerOtherEvent(), SinkerOtherEvent::NAME);
            });

            go(function () use ($process) {
                while (true) {
                    $this->checkMasterPid($process);
                    KafkaLog::getInstance()
                            ->info(sprintf('pid:%d,Check if the service master process exists every %s seconds...' . PHP_EOL,
                                getmypid(), 60));
                    co::sleep(60);
                }
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
        $process = new Process(function (Process $process) use ($index) {
            swoole_set_process_name($this->getProcessName());
            dispatch(new SetKafkaProcessNameEvent($index), SetKafkaProcessNameEvent::NAME);
            Runtime::enableCoroutine(true, SWOOLE_HOOK_ALL);

            Process::signal(SIGINT, function () use ($process) {
                go(function () use ($process) {
                    $this->leaveGroup($process);
                });
            });

            Process::signal(SIGTERM, function () use ($process) {
                go(function () use ($process) {
                    $this->leaveGroup($process);
                });
            });

            go(function () use ($index) {
                while (true) {
                    if (ClientKafka::getInstance()->isJoined()) {
                        break;
                    }
                    \co::sleep(1);
                }
                $path = $this->getChildSockFile($index);
                @unlink($path);
                $socket = new Socket(AF_UNIX, SOCK_STREAM, 0);
                $socket->bind($path);
                $socket->listen();
                while (true) {
                    $client = $socket->accept();
                    if ($client === false) {
                        \co::sleep(1);
                    } else {
                        $msg = $this->getAfUnixMessage($client);
                        $msg = json_decode($msg, true);
                        ['rpc' => $rpc, 'method' => $method] = $msg;
                        if (isset($msg['params'])) {
                            $params = $msg['params'];
                        }

                        if (!isset($params)) {
                            $result = call_user_func([(new $rpc), $method]);
                        } else {
                            $result = call_user_func([(new $rpc), $method], $params);
                        }

                        $data = json_encode($result);
                        $package = pack('N', strlen($data)) . $data;
                        $client->send($package);
                        $client->close();
                    }
                }
            });

            // Heartbeat
            go(function () use ($process) {
                while (true) {
                    $this->checkMasterPid($process);
                    KafkaLog::getInstance()
                            ->info(sprintf('pid:%d,Check if the service master process exists every %s seconds...' . PHP_EOL,
                                getmypid(), 60));
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

    /**
     * @param Process $process
     */
    public function leaveGroup(Process $process): void
    {
        if (ClientKafka::getInstance()->isJoined()) {
            LeaveGroupApi::getInstance()
                         ->leave(App::$commonConfig->getGroupId(),
                             ClientKafka::getInstance()->getMemberId());
        }
        $process->exit(0);
    }

    /**
     * @param Process $process
     */
    public function checkMasterPid(Process $process)
    {
        static $memory_limit;
        // check memory，free memory
        if (empty($memory_limit) && preg_match('/(?<member_limit_mb>\d+)M/', ini_get('memory_limit'), $matches)) {
            $memory_limit = $matches['member_limit_mb'];
        }

        $memory = round(memory_get_usage() / 1024 / 1024, 2);
        if ($memory / $memory_limit > 0.9) {
            dispatch(new ProcessExitEvent(), ProcessExitEvent::NAME);
            $process->exit(0);
        }

        if (!Process::kill($this->masterPid, 0)) {
            dispatch(new ProcessExitEvent(), ProcessExitEvent::NAME);
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
            KafkaLog::getInstance()->emergency("rebootKafkaProcess: {$index}={$new_pid} Done");

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
            KafkaLog::getInstance()->emergency("rebootSinkerProcess: {$index}={$new_pid} Done");

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
            // Warning: when the child process restarts, the unixfile of the master process will be removed
            $this->createMasterUnixFile();
            $this->createMasterUnixFileAccpet();
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