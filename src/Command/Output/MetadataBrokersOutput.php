<?php
declare(strict_types=1);

namespace Kafka\Command\Output;

use Symfony\Component\Console\Style\SymfonyStyle;

class MetadataBrokersOutput extends AbstractOutput
{
    public function output(SymfonyStyle $io, $data)
    {
        $headers = [
            'node-id',
            'host',
            'port'
        ];
        $io->table($headers, $data);
    }
}