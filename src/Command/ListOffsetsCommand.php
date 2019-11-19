<?php
declare(strict_types=1);

namespace Kafka\Command;

use App\App;
use Kafka\Api\DescribeGroupsApi;
use Kafka\Api\ListOffsetsApi;
use Kafka\Api\OffsetCommitApi;
use Kafka\Api\OffsetFetchApi;
use Kafka\Command\Output\DescribeGruopsOutput;
use Kafka\Command\Output\ListOffsetsOutput;
use Kafka\Enum\CompressionCodecEnum;
use Kafka\Enum\ProtocolErrorEnum;
use Kafka\Event\StartBeforeEvent;
use Kafka\Kafka;
use Kafka\Manager\MetadataManager;
use Kafka\Protocol\Request\DescribeGroupsRequest;
use Kafka\Protocol\Request\FindCoordinatorRequest;
use Kafka\Protocol\Request\Produce\DataProduce;
use Kafka\Protocol\Request\Produce\MessageProduce;
use Kafka\Protocol\Request\Produce\MessageSetProduce;
use Kafka\Protocol\Request\Produce\TopicDataProduce;
use Kafka\Protocol\Request\ProduceRequest;
use Kafka\Protocol\Response\DescribeGroupsResponse;
use Kafka\Protocol\Response\FindCoordinatorResponse;
use Kafka\Protocol\Response\ProduceResponse;
use Kafka\Protocol\Type\Bytes32;
use Kafka\Protocol\Type\Int16;
use Kafka\Protocol\Type\Int32;
use Kafka\Protocol\Type\Int64;
use Kafka\Protocol\Type\Int8;
use Kafka\Protocol\Type\String16;
use Kafka\Server\KafkaCServer;
use Kafka\Socket\Socket;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

class ListOffsetsCommand extends Command
{
    protected static $defaultName = 'kafka.list_offsets';

    protected function configure()
    {
        $this
            ->setDescription('Browse the offset of a topic in kafka')
            ->setHelp('Browse the offset of a topic in kafka...')
            ->addOption(
                'topic',
                't',
                InputOption::VALUE_REQUIRED,
                'Which topic is subscribed by the consumer group?'
            )->addArgument(
                'timestamp',
                InputArgument::OPTIONAL,
                'timestamp, 0=>all, -1=>max, -2=>min, 1573758000=>time',
                0
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
        $topic = $input->getOption('topic');
        $timestamp = (int)$input->getArgument('timestamp');
        if ($timestamp === 0) {
            $inTimestamp = -1;
        } else {
            $inTimestamp = $timestamp;
        }

        MetadataManager::getInstance()->registerConfig()->registerMetadataInfo([$topic]);

        $partitions = Kafka::getInstance()->getPartitions();
        $partitions = $partitions[$topic];
        $data = ListOffsetsApi::getInstance()->getListOffsets($topic, $partitions, $inTimestamp);

        $data = [
            'data'      => $data,
            'timestamp' => $timestamp
        ];

        (new ListOffsetsOutput())->output(new SymfonyStyle($input, $output), $data);
    }
}
