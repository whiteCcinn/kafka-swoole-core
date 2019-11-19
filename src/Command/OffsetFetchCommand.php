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
use Kafka\Command\Output\OffsetFetchOutput;
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

class OffsetFetchCommand extends Command
{
    protected static $defaultName = 'kafka.offse_fetch';

    protected function configure()
    {
        $this
            ->setDescription('Submit offset consumer group')
            ->setHelp('Submit offset consumer group...')
            ->addOption(
                'topic',
                't',
                InputOption::VALUE_REQUIRED,
                'Which topic is subscribed by the consumer group?'
            )->addOption(
                'group',
                'g',
                InputOption::VALUE_REQUIRED,
                'Which consumer group?'
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
        $group = $input->getOption('group');
        $topic = $input->getOption('topic');

        MetadataManager::getInstance()->registerConfig()->registerMetadataInfo([$topic]);

        $partitions = Kafka::getInstance()->getPartitions();
        $partitions = $partitions[$topic];

        // FindCoordinator...
        $findCoordinatorRequest = new FindCoordinatorRequest();
        $data = $findCoordinatorRequest->setKey(String16::value(App::$commonConfig->getGroupId()))->pack();
        ['host' => $host, 'port' => $port] = Kafka::getInstance()->getRandBroker();
        $socket = new Socket();
        $socket->connect($host, $port)->send($data);
        $socket->revcByKafka($findCoordinatorRequest);
        $socket->close();

        /** @var FindCoordinatorResponse $response */
        $response = $findCoordinatorRequest->response;
        if ($response->getErrorCode()->getValue() !== ProtocolErrorEnum::NO_ERROR) {
            throw new FindCoordinatorRequestException(sprintf('FindCoordinatorRequest request error, the error message is: %s',
                ProtocolErrorEnum::getTextByCode($response->getErrorCode()->getValue())));
        }
        $host = $response->getHost()->getValue();
        $port = (int)$response->getPort()->getValue();

        $allPartitions = [
            $topic => $partitions,
        ];
        $result = OffsetFetchApi::getInstance()->setHost($host)->setPort($port)
                                ->getOffsetByGroupAndTopicAndPartitions($group, $allPartitions);

        (new OffsetFetchOutput())->output(new SymfonyStyle($input, $output), $result);
    }
}
