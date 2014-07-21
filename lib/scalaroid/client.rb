module Scalaroid
  class Client
    def initialize(options={})
      @conn = Scalaroid::TransactionSingleOp.new()
    end

    def get(key)
      begin
        @conn.read(key)
      rescue Scalaroid::NotFoundError
        nil
      end
    end

    def set(key, value)
      @conn.write(key, value)
    end
  end
end
