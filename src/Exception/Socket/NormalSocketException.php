<?php
declare(strict_types=1);

namespace Kafka\Exception\Socket;

use Exception;

final class NormalSocketException extends Exception
{
    public static function invalidLength(int $length, int $maxLength): self
    {
        return new self(sprintf('Invalid length %d given, it should be lesser than or equals to %d', $length,
            $maxLength));
    }

    public static function notReadable(int $length): self
    {
        return new self(sprintf('Could not read %d bytes from stream (not readable)', $length));
    }

    public static function timedOut(int $length): self
    {
        return new self(sprintf('Timed out reading %d bytes from stream', $length));
    }

    public static function timedOutWithRemainingBytes(int $length, int $remainingBytes): self
    {
        return new self(sprintf('Timed out while reading %d bytes from stream, %d bytes are still needed', $length,
            $remainingBytes));
    }

    public static function unexpectedEOF(int $length): self
    {
        return new self(sprintf('Unexpected EOF while reading %d bytes from stream (no data)', $length));
    }

    public static function connectFail($host, $port, $errstr, $errno): NormalSocketConnectException
    {
        return new NormalSocketConnectException(sprintf('Connected fail. host: %s , port: %d，errstr: %s, errno %d',
            $host, $port, $errstr, $errno));
    }
}