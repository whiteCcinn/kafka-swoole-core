<?php
declare(strict_types=1);

namespace Kafka\Command\Output;

use Symfony\Component\Console\Style\SymfonyStyle;

class MetadataTopicsOutput extends AbstractOutput
{
    public function output(SymfonyStyle $io, $data)
    {
        $headers = [
            'topic',
            'partition',
            'leader-id',
            'replica-nodes',
            'isr-nodes'
        ];

        $data = current($data);
        $topic = $data['name'];
        $item = [];
        foreach ($data['partitions'] as $partitionInfo) {
            $item[] = [
                $topic,
                $partitionInfo['partitionIndex'],
                $partitionInfo['leaderId'],
                implode(',',$partitionInfo['replicaNodes']),
                implode(',',$partitionInfo['isrNodes']),
            ];
        }

        $io->table($headers, $item);
    }
}