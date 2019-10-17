<?php
declare(strict_types=1);

namespace Kafka\Command\Output;

use Symfony\Component\Console\Style\SymfonyStyle;

abstract class AbstractOutput
{
    abstract public function output(SymfonyStyle $io, $data);
}