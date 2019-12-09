<?php
declare(strict_types=1);

namespace Kafka\Command;

use Kafka\Api\ListOffsetsApi;
use Kafka\Command\Output\ProducerOutput;
use Kafka\Enum\CompressionCodecEnum;
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

class ProducerCommand extends Command
{
    protected static $defaultName = 'kafka.produce';

    protected function configure()
    {
        $this
            ->setDescription('Send a message')
            ->setHelp('This command will help you send separate messages to a topic...')
            ->addOption(
                'topic',
                't',
                InputOption::VALUE_OPTIONAL,
                'Which is the topic you want to send?'
            )->addOption(
                'partition',
                'p',
                InputOption::VALUE_OPTIONAL,
                'Which is the topic you want to send to partition?'
            )->addOption(
                'key',
                'k',
                InputOption::VALUE_OPTIONAL,
                'Which is the topic you want to send to partition by key?'
            )->addOption(
                'compress',
                'c',
                InputOption::VALUE_OPTIONAL,
                'Which one do you want to compress: 0: normal 1:gzip 2:snappy 3:lz4')
            ->addOption(
                'repeat',
                'r',
                InputOption::VALUE_OPTIONAL,
                'Number of message repeats',
                1)
            ->addArgument(
                'message',
                InputArgument::REQUIRED,
                'The message you wish to send.'
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
        $partition = $input->getOption('partition');
        $key = $input->getOption('key');
        $compress = (int)$input->getOption('compress');
        $repeat = (int)$input->getOption('repeat');
        $message = $input->getArgument('message');

        Send:
        MetadataManager::getInstance()->registerConfig()->registerMetadataInfo([$topic]);
        $topicPartitionLeaders = Kafka::getInstance()->getTopicsPartitionLeader();
        $topicPartition = isset($topicPartitionLeaders[$topic]) ? array_keys($topicPartitionLeaders[$topic]) : [0];
        $topicPartitionLeader = isset($topicPartitionLeaders[$topic]) ? $topicPartitionLeaders[$topic] : current($topicPartitionLeaders);
        // Range
        if ($partition === null && $key === null) {
            shuffle($topicPartition);
            $assignPartition = current($topicPartition);
        } elseif ($partition === null && $key !== null) {
            $assignPartition = crc32(md5($key)) % count($topicPartition);
        } else {
            // if ($partition !== null && $key !== null) || ($partition !== null && $key === null)
            $assignPartition = (int)$partition;
        }

        // Compress
        if ($compress === CompressionCodecEnum::NORMAL) {
            $attributes = Int8::value(CompressionCodecEnum::NORMAL);
        } elseif ($compress === CompressionCodecEnum::SNAPPY) {
            $attributes = Int8::value(CompressionCodecEnum::SNAPPY);
        } elseif ($compress === CompressionCodecEnum::GZIP) {
            $attributes = Int8::value(CompressionCodecEnum::GZIP);
        } else {
            throw new \RuntimeException('Todo Lz4 Compress');
        }
//        $listOffsets = ListOffsetsApi::getInstance()->getListOffsets($topic, [$assignPartition]);
//        ['highWatermark' => $highWatermark] = current($listOffsets);

        $messageSets = [];

        while ($repeat > 0) {
            $messageSets[] = (new MessageSetProduce())->setOffset(Int64::value(-1))
                                                      ->setMessage(
                                                          (new MessageProduce())->setAttributes($attributes)
                                                                                ->setValue(Bytes32::value($message))
                                                      );
            $repeat--;
        }

        $protocol = new ProduceRequest();
        $protocol->setAcks(Int16::value(1))
                 ->setTimeout(Int32::value(1 * 1000))
                 ->setTopicData([
                     (new TopicDataProduce())->setTopic(String16::value($topic))
                                             ->setData([
                                                 (new DataProduce())->setPartition(Int32::value($assignPartition))
                                                                    ->setMessageSet($messageSets)
                                             ])
                 ]);
        $data = $protocol->pack();
        $socket = Kafka::getInstance()->getSocketByNodeId($topicPartitionLeader[$assignPartition]);
        $socket->send($data);
        $socket->revcByKafka($protocol);
        /** @var ProduceResponse $responses */
        $responses = $protocol->response;
        foreach ($responses->getResponses() as $response) {
            $info = $response->getPartitionResponses()[0];
            if (
                in_array($info->getErrorCode()->getValue(),
                    [ProtocolErrorEnum::UNKNOWN_TOPIC_OR_PARTITION, ProtocolErrorEnum::NOT_LEADER_FOR_PARTITION])
                &&
                $info->getBaseOffset()->getValue() === -1
            ) {
                goto Send;
            }
        }
        $result = $responses->toArray();
        (new ProducerOutput())->output(new SymfonyStyle($input, $output), $result);
    }
}
