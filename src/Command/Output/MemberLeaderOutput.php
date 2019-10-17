<?php
declare(strict_types=1);

namespace Kafka\Command\Output;

use Symfony\Component\Console\Style\SymfonyStyle;

class MemberLeaderOutput extends AbstractOutput
{
    public function output(SymfonyStyle $io, $data)
    {
        $headers = [
            'consumer-group-leaderId',
        ];
        $io->table($headers, [[$data]]);
    }
}