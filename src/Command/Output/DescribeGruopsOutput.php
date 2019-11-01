<?php
declare(strict_types=1);

namespace Kafka\Command\Output;

use Symfony\Component\Console\Style\SymfonyStyle;

class DescribeGruopsOutput extends AbstractOutput
{
    public function output(SymfonyStyle $io, $data)
    {
        $headers = [
            'groupId',
            'groupState',
            'protocolType',
            'protocolData'
        ];
        $baseInfo = [
            [
                $data['groupId'],
                $data['groupState'],
                $data['protocolType'] !== '' ? $data['protocolType'] : 'null',
                $data['protocolData'] !== '' ? $data['protocolData'] : 'null',
            ]
        ];
        $io->title('DescribeGruops-BaseInfo');
        $io->table($headers, $baseInfo);

        foreach ($data['members'] as $memberInfoItem) {
            $memberTopicsInfo = [];
            $headers = [
                'memberId',
                'clientId',
                'clientHost',
                'topcic',
                'paritions',
            ];
            foreach ($memberInfoItem['memberAssignment']['partitionsAssignment'] as $topicPartitions) {
                $info = [
                    $memberInfoItem['memberId'],
                    $memberInfoItem['clientId'],
                    $memberInfoItem['clientHost'],
                    $topicPartitions['topic'],
                    implode(',', $topicPartitions['partitions']),
                ];
                $memberTopicsInfo[] = $info;
            }
            $io->table($headers, $memberTopicsInfo);
        }
    }
}