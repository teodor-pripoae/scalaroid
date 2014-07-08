module Scalaroid
  # Non-transactional operations on the replicated DHT of Scalaris
  class ReplicatedDHT
    # Create a new object using the given connection.
    def initialize(conn = JSONConnection.new())
      @conn = conn
    end

    # Tries to delete the value at the given key.
    #
    # WARNING: This function can lead to inconsistent data (e.g. deleted items
    # can re-appear). Also when re-creating an item the version before the
    # delete can re-appear.
    #
    # returns the number of successfully deleted items
    # use get_last_delete_result() to get more details
    def delete(key, timeout = 2000)
      result_raw = @conn.call(:delete, [key, timeout])
      result = @conn.class.process_result_delete(result_raw)
      @lastDeleteResult = result[:results]
      if result[:success] == true
        return result[:ok]
      elsif result[:success] == :timeout
        raise TimeoutError.new(result_raw)
      else
        raise UnknownError.new(result_raw)
      end
    end

    # Returns the result of the last call to delete().
    #
    # NOTE: This function traverses the result list returned by Scalaris and
    # therefore takes some time to process. It is advised to store the returned
    # result object once generated.
    def get_last_delete_result
      @conn.class.create_delete_result(@lastDeleteResult)
    end

    include InternalScalarisNopClose
  end
end
