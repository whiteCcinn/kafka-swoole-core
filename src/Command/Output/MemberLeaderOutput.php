<?php
declare(strict_types=1);

namespace Kafka\Command\Output;

use Symfony\Component\Console\Style\SymfonyStyle;

class MemberLeaderOutput extends AbstractOutput
{
    public function output(SymfonyStyle $io, $data)
    {

        $leaderTableValue = [];
        $memberTableValue = [];
        foreach ($data as $item) {
            if ($item['isLeader']) {
                $leaderTableValue[] = [$item['memberId']];
            }
            $memberTableValue[] = [$item['memberId']];
        }
        $headers = [
            'consumer-group-leaderId',
        ];
        $io->table($headers, $leaderTableValue);

        $headers = [
            'consumer-group-membersId',
        ];
        $io->table($headers, $memberTableValue);
    }
}