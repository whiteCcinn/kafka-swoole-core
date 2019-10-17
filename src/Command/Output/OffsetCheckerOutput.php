<?php
declare(strict_types=1);

namespace Kafka\Command\Output;

use Symfony\Component\Console\Style\SymfonyStyle;

class OffsetCheckerOutput extends AbstractOutput
{
    public function output(SymfonyStyle $io, $data)
    {
        $headers = [
            'topic',
            'partition',
            'current-offset',
            'kafka-max-offset',
            'remaining-count'
        ];
        $io->table($headers, $data);
    }
}