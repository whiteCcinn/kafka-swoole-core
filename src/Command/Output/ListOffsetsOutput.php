<?php
declare(strict_types=1);

namespace Kafka\Command\Output;

use Symfony\Component\Console\Style\SymfonyStyle;

class ListOffsetsOutput extends AbstractOutput
{
    public function output(SymfonyStyle $io, $data)
    {
        $timestamp = $data['timestamp'];
        $data = $data['data'];
        if ($timestamp === 0) {
            $headers = [
                'topic',
                'partition',
                'earliest',
                'highWatermark'
            ];
        } else {
            $headers = [
                'topic',
                'partition',
                'nextOffset',
            ];
            foreach ($data as &$item) {
                $item['nextOffset'] = $item['highWatermark'];
                unset($item['earliest'], $item['highWatermark']);
            }
            unset($item);
        }
        $io->table($headers, $data);
    }
}