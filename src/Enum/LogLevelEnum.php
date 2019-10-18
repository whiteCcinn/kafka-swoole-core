<?php
declare(strict_types=1);

namespace Kafka\Enum;

/**
 * Class LogLevelEnum
 *
 * @package Kafka\Enum
 */
class LogLevelEnum extends AbstractEnum
{
    /**
     * Detailed debug information
     *
     * @message("DEBUG")
     */
    public const DEBUG = 100;

    /**
     * Interesting events
     *
     * Examples: User logs in, SQL logs.
     *
     * @message("INFO")
     */
    public const INFO = 200;

    /**
     * Uncommon events
     *
     * @message("NOTICE")
     */
    public const NOTICE = 250;

    /**
     * Exceptional occurrences that are not errors
     *
     * Examples: Use of deprecated APIs, poor use of an API,
     * undesirable things that are not necessarily wrong.
     *
     * @message("WARNING")
     */
    public const WARNING = 300;

    /**
     * Runtime errors
     *
     * @message("ERROR")
     */
    public const ERROR = 400;

    /**
     * Critical conditions
     *
     * Example: Application component unavailable, unexpected exception.
     *
     * @message("CRITICAL")
     */
    public const CRITICAL = 500;

    /**
     * Action must be taken immediately
     *
     * Example: Entire website down, database unavailable, etc.
     * This should trigger the SMS alerts and wake you up.
     *
     * @message("ALERT")
     */
    public const ALERT = 550;

    /**
     * Urgent alert.
     * @message("EMERGENCY")
     */
    public const EMERGENCY = 600;
}