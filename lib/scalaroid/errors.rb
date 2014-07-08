module Scalaroid
  # Base class for errors in the scalaris package.
  class ScalarisError < StandardError
  end

  # Exception that is thrown if a the commit of a write operation on a Scalaris
  # ring fails.
  class AbortError < ScalarisError
    attr_reader :raw_result
    attr_reader :failed_keys
    def initialize(raw_result, failed_keys)
      @raw_result = raw_result
      @failed_keys = failed_keys
    end

    def to_s
      @raw_result
    end
  end

  # Exception that is thrown if an operation on a Scalaris ring fails because
  # a connection does not exist or has been disconnected.
  class ConnectionError < ScalarisError
    include InternalScalarisSimpleError
  end

  # Exception that is thrown if a test_and_set operation on a Scalaris ring
  # fails because the old value did not match the expected value.
  class KeyChangedError < ScalarisError
    attr_reader :raw_result
    attr_reader :old_value
    def initialize(raw_result, old_value)
      @raw_result = raw_result
      @old_value = old_value
    end

    def to_s
      @raw_result + ", old value: " + @old_value
    end
  end

  # Exception that is thrown if a delete operation on a Scalaris ring fails
  # because no Scalaris node was found.
  class NodeNotFoundError < ScalarisError
    include InternalScalarisSimpleError
  end

  # Exception that is thrown if a read operation on a Scalaris ring fails
  # because the key did not exist before.
  class NotFoundError < ScalarisError
    include InternalScalarisSimpleError
  end

  # Exception that is thrown if a add_del_on_list operation on a scalaris ring
  # fails because the participating values are not lists.
  class NotAListError < ScalarisError
    include InternalScalarisSimpleError
  end

  # Exception that is thrown if a add_del_on_list operation on a scalaris ring
  # fails because the participating values are not numbers.
  class NotANumberError < ScalarisError
    include InternalScalarisSimpleError
  end

  # Exception that is thrown if a read or write operation on a Scalaris ring
  # fails due to a timeout.
  class TimeoutError < ScalarisError
    include InternalScalarisSimpleError
  end

  # Generic exception that is thrown during operations on a Scalaris ring, e.g.
  # if an unknown result has been returned.
  class UnknownError < ScalarisError
    include InternalScalarisSimpleError
  end
end
