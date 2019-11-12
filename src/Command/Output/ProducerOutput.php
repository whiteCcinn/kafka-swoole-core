<?php
declare(strict_types=1);

namespace Kafka\Command\Output;

use Kafka\Enum\ProtocolErrorEnum;
use Symfony\Component\Console\Style\SymfonyStyle;

class ProducerOutput extends AbstractOutput
{
    public function output(SymfonyStyle $io, $data)
    {
        $headers = [
            'topic',
            'partition',
            'errorCode',
            'errorMsg',
            'baseOffset(Maximum offset before sending this request)',
        ];

        foreach ($data['responses'] as $item) {
            $infos[] = [
                $item['topic'],
                $item['partition_responses'][0]['partition'],
                $item['partition_responses'][0]['errorCode'],
                ProtocolErrorEnum::getTextByCode($item['partition_responses'][0]['errorCode']),
                $item['partition_responses'][0]['baseOffset'],
            ];
        }

        $io->table($headers, $infos);
    }
}