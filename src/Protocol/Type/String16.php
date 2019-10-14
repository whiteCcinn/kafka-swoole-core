<?php
declare(strict_types=1);

namespace Kafka\Protocol\Type;

class String16 extends AbstractType
{
    protected static $wrapperProtocol = 'n';

    /** @var mixed $value */
    protected $value;

    /**
     * @return mixed
     */
    public function getValue()
    {
        return $this->value;
    }

    /**
     * @param mixed $value
     *
     * @return $this
     */
    public function setValue($value)
    {
        $this->value = $value;

        return $this;
    }
}