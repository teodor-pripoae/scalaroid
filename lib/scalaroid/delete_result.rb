module Scalaroid
  # Stores the result of a delete operation.
  class DeleteResult
    attr_reader :ok
    attr_reader :locks_set
    attr_reader :undefined
    def initialize(ok, locks_set, undefined)
      @ok = ok
      @locks_set = locks_set
      @undefined = undefined
    end
  end
end
