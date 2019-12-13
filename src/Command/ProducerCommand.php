<?php
declare(strict_types=1);

namespace Kafka\Command;

use App\App;
use Kafka\Api\ListOffsetsApi;
use Kafka\Api\ProducerApi;
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
                'brokers_list',
                'l',
                InputOption::VALUE_OPTIONAL,
                'where are you want to send to kafka\'s cluster'
            )
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

        $brokersList = $input->getOption('brokers_list');
        $topic = $input->getOption('topic');
        $partition = (int)$input->getOption('partition');
        $key = $input->getOption('key');
        $compress = (int)$input->getOption('compress');
        $repeat = (int)$input->getOption('repeat');
        $message = $input->getArgument('message');
        MetadataManager::getInstance()->registerConfig()->registerMetadataInfo([$topic]);

        while ($repeat > 0) {
            $messages[] = $message;
            $repeat--;
        }

        if(empty($brokersList)){
            $brokersList = App::$commonConfig->getMetadataBrokerList();
        }
        $conn = 'producer-command';
        ProducerApi::getInstance()->setBrokerListMap($conn, $brokersList);
        ProducerApi::getInstance()->produce($conn, $topic, $partition, $key, $messages, $compress);
        $responses = ProducerApi::getInstance()->getOrSetLastResponse();
        $result = $responses->toArray();
        (new ProducerOutput())->output(new SymfonyStyle($input, $output), $result);
    }
}
