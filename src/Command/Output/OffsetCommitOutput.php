<?php
declare(strict_types=1);

namespace Kafka\Command\Output;

use Symfony\Component\Console\Style\SymfonyStyle;

class OffsetCommitOutput extends AbstractOutput
{
    public function output(SymfonyStyle $io, $data)
    {
        $before = $data['before'];
        $after = $data['after'];

        $headers = [
            'topic',
            'partition',
            'offset'
        ];
        $io->title('Before the change：');
        $io->table($headers, $before);

        $io->title('After the change：');
        $io->table($headers, $after);
    }
}