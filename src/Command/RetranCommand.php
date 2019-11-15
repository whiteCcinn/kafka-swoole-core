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
use Kafka\Storage\RedisStorage;
use Kafka\Storage\StorageAdapter;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

class RetranCommand extends Command
{
    protected static $defaultName = 'retran';

    protected function configure()
    {
        $this
            ->setDescription('The retransmission message')
            ->setHelp('Retransmit all data in processing...');
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
        go(function () use ($output) {
            /** @var StorageAdapter $adapter */
            $adapter = StorageAdapter::getInstance();
            /** @var RedisStorage $storage */
            $storage = RedisStorage::getInstance();
            $adapter->setAdaptee($storage);

            if ($adapter->retran()) {
                $output->write('<info>Retraned!</info>', true);
            } else {
                $output->write('<error>Retran failed!</error>', true);
            }
        });
    }
}
