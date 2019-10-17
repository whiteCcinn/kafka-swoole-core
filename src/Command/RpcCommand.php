<?php
declare(strict_types=1);

namespace Kafka\Command;

use Kafka\Command\Output\AbstractOutput;
use Kafka\Command\Output\KafkaLagOutput;
use Kafka\Command\Output\MemberLeaderOutput;
use Kafka\Command\Output\MetadataBrokersOutput;
use Kafka\Command\Output\MetadataTopicsOutput;
use Kafka\Command\Output\OffsetCheckerOutput;
use Kafka\Enum\ProtocolErrorEnum;
use Kafka\Event\StartBeforeEvent;
use Kafka\Kafka;
use Kafka\Manager\MetadataManager;
use Kafka\Protocol\Request\Produce\DataProduce;
use Kafka\Protocol\Request\Produce\MessageProduce;
use Kafka\Protocol\Request\Produce\MessageSetProduce;
use Kafka\Protocol\Request\Produce\TopicDataProduce;
use Kafka\Protocol\Request\ProduceRequest;
use Kafka\Protocol\Response\ProduceResponse;
use Kafka\Protocol\Type\Bytes32;
use Kafka\Protocol\Type\Int16;
use Kafka\Protocol\Type\Int32;
use Kafka\Protocol\Type\Int64;
use Kafka\Protocol\Type\String16;
use Kafka\RPC\KafkaOffsetRpc;
use Kafka\RPC\MemberLeaderRpc;
use Kafka\RPC\MetadataRpc;
use Kafka\RPC\StorageCapacityRpc;
use Kafka\Server\KafkaCServer;
use Kafka\Socket\Socket;
use Kafka\Support\Str;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

class RpcCommand extends Command
{
    protected static $defaultName = 'rpc';

    private const SELF_UNIX_FILE = 'client.sock';

    private $commandType = [
        'kafka_lag'        => [
            'rpc'    => KafkaOffsetRpc::class,
            'method' => 'getKafkaLag',
        ],
        'offset_checker'   => [
            'rpc'    => KafkaOffsetRpc::class,
            'method' => 'getOffsetChecker',
            'output' => OffsetCheckerOutput::class
        ],
        'block_size'       => [
            'rpc'    => StorageCapacityRpc::class,
            'method' => 'getBlockSize'
        ],
        'member_leader'    => [
            'rpc'    => MemberLeaderRpc::class,
            'method' => 'getMemberLeaderId',
            'output' => MemberLeaderOutput::class
        ],
        'metadata_brokers' => [
            'rpc'    => MetadataRpc::class,
            'method' => 'getBrokerList',
            'output' => MetadataBrokersOutput::class
        ],
        'metadata_topics'  => [
            'rpc'    => MetadataRpc::class,
            'method' => 'getTopics',
            'output' => MetadataTopicsOutput::class
        ]
    ];

    protected function configure()
    {
        $this
            ->setDescription('Built-in runtime RPC command')
            ->setHelp(implode(PHP_EOL,
                array_merge(['The following are the built-in RPC command options：'], array_keys($this->commandType))))
            ->addArgument(
                'type',
                InputArgument::REQUIRED,
                'which you want to execute command?'
            );

    }

    /**
     * @param InputInterface  $input
     * @param OutputInterface $output
     *
     * @return int|null|void
     * @throws \Exception
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $io = new SymfonyStyle($input, $output);
        $command = $input->getArgument('type');
        $path = $this->getSelfClientSockFile();
        $socket = new \Co\Socket(AF_UNIX, SOCK_STREAM, 0);
        $socket->bind($path);
        go(function () use ($socket, $command, $io) {
            $cmd = [
                'role'   => \Kafka\Enum\RpcRoleEnum::EXTERNAL,
                'rpc'    => $this->commandType[$command]['rpc'],
                'method' => $this->commandType[$command]['method']
            ];
            $data = json_encode($cmd);
            $package = pack('N', strlen($data)) . $data;
            $socket->connect(KafkaCServer::getMatserSockFile());
            $socket->send($package);

            $len = $socket->recv(4, 3);
            $len = unpack('N', $len);
            $len = is_array($len) ? current($len) : $len;
            $msg = $socket->recv($len, 3);
            $socket->close();
            $ret = json_decode($msg, true);
            if (isset($this->commandType[$command]['output'])) {
                /** @var AbstractOutput $outputClass */
                $outputClass = $this->commandType[$command]['output'];
                $outputInstance = new $outputClass;
                $outputInstance->output($io, $ret);
            } else {
                if (is_array($ret)) {
                    print_r($ret);
                } elseif (is_bool($ret)) {
                    var_dump($ret);
                } else {
                    echo $ret . PHP_EOL;
                }
            }
        });
    }

    private function getSelfClientSockFile(): string
    {
        $dir = env('SERVER_AF_UNIX_DIR');
        if (!Str::endsWith($dir, '/')) {
            $dir .= '/';
        }

        $path = $dir . sprintf(self::SELF_UNIX_FILE);

        return $path;
    }
}
