module Scalaroid
  # Write or read operations on Scalaris inside a transaction.
  class Transaction
    # Create a new object using the given connection
    def initialize(conn = JSONConnection.new())
      @conn = conn
      @tlog = nil
    end

    # Returns a new ReqList object allowing multiple parallel requests.
    def new_req_list(other = nil)
      @conn.class.new_req_list_t(other)
    end

    # Issues multiple parallel requests to Scalaris.
    # Request lists can be created using new_req_list().
    # The returned list has the following form:
    # [{'status': 'ok'} or {'status': 'ok', 'value': xxx} or
    # {'status': 'fail', 'reason': 'timeout' or 'abort' or 'not_found'}].
    # The elements of this list can be processed with process_result_read(),
    # process_result_write() and process_result_commit().
    def req_list(reqlist)
      if @tlog == nil
        result = @conn.call(:req_list, [reqlist.get_requests()])
      else
        result = @conn.call(:req_list, [@tlog, reqlist.get_requests()])
      end
      result = @conn.class.process_result_req_list_t(result)
      @tlog = result[:tlog]
      result = result[:result]
      if reqlist.is_commit()
        _process_result_commit(result[-1])
        # transaction was successful: reset transaction log
        @tlog = nil
      end
      result
    end

    # Processes a result element from the list returned by req_list() which
    # originated from a read operation.
    # Returns the read value on success.
    # Raises the appropriate exceptions if a failure occurred during the
    # operation.
    # Beware: lists of (small) integers may be (falsely) returned as a string -
    # use str_to_list() to convert such strings.
    def process_result_read(result)
      @conn.class.process_result_read(result)
    end

    # Processes a result element from the list returned by req_list() which
    # originated from a write operation.
    # Raises the appropriate exceptions if a failure occurred during the
    # operation.
    def process_result_write(result)
      @conn.class.process_result_write(result)
    end

    # Processes a result element from the list returned by req_list() which
    # originated from a add_del_on_list operation.
    # Raises the appropriate exceptions if a failure occurred during the
    # operation.
    def process_result_add_del_on_list(result)
      @conn.class.process_result_add_del_on_list(result)
    end

    # Processes a result element from the list returned by req_list() which
    # originated from a add_on_nr operation.
    # Raises the appropriate exceptions if a failure occurred during the
    # operation.
    def process_result_add_on_nr(result)
      @conn.class.process_result_add_on_nr(result)
    end

    # Processes a result element from the list returned by req_list() which
    # originated from a test_and_set operation.
    # Raises the appropriate exceptions if a failure occurred during the
    # operation.
    def process_result_test_and_set(result)
      @conn.class.process_result_test_and_set(result)
    end

    # Processes a result element from the list returned by req_list() which
    # originated from a commit operation.
    # Raises the appropriate exceptions if a failure occurred during the
    # operation.
    def _process_result_commit(result)
      @conn.class.process_result_commit(result)
    end

    private :_process_result_commit

    # Issues a commit operation to Scalaris validating the previously
    # created operations inside the transaction.
    def commit
      result = req_list(new_req_list().add_commit())[0]
      _process_result_commit(result)
      # reset tlog (minor optimization which is not done in req_list):
      @tlog = nil
    end

    # Aborts all previously created operations inside the transaction.
    def abort
      @tlog = nil
    end

    # Issues a read operation to Scalaris, adds it to the current
    # transaction and returns the result.
    # Beware: lists of (small) integers may be (falsely) returned as a string -
    # use str_to_list() to convert such strings.
    def read(key)
      result = req_list(new_req_list().add_read(key))[0]
      return process_result_read(result)
    end

    # Issues a write operation to Scalaris and adds it to the current
    # transaction.
    def write(key, value, binary = false)
      result = req_list(new_req_list().add_write(key, value, binary))[0]
      _process_result_commit(result)
    end

    # Issues a add_del_on_list operation to scalaris and adds it to the
    # current transaction.
    # Changes the list stored at the given key, i.e. first adds all items in
    # to_add then removes all items in to_remove.
    # Both, to_add and to_remove, must be lists.
    # Assumes en empty list if no value exists at key.
    def add_del_on_list(key, to_add, to_remove)
      result = req_list(new_req_list().add_add_del_on_list(key, to_add, to_remove))[0]
      process_result_add_del_on_list(result)
    end

    # Issues a add_on_nr operation to scalaris and adds it to the
    # current transaction.
    # Changes the number stored at the given key, i.e. adds some value.
    # Assumes 0 if no value exists at key.
    def add_on_nr(key, to_add)
      result = req_list(new_req_list().add_add_on_nr(key, to_add))[0]
      process_result_add_on_nr(result)
    end

    # Issues a test_and_set operation to scalaris and adds it to the
    # current transaction.
    # Atomic test and set, i.e. if the old value at key is old_value, then
    # write new_value.
    def test_and_set(key, old_value, new_value)
      result = req_list(new_req_list().add_test_and_set(key, old_value, new_value))[0]
      process_result_test_and_set(result)
    end

    include InternalScalarisNopClose
  end
end
