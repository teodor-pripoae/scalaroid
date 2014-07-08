module Scalaroid
  # Single write or read operations on Scalaris.
  class TransactionSingleOp
    # Create a new object using the given connection
    def initialize(conn = JSONConnection.new())
      @conn = conn
    end

    # Returns a new ReqList object allowing multiple parallel requests.
    def new_req_list(other = nil)
      @conn.class.new_req_list_t(other)
    end

    # Issues multiple parallel requests to scalaris; each will be committed.
    # NOTE: The execution order of multiple requests on the same key is
    # undefined!
    # Request lists can be created using new_req_list().
    # The returned list has the following form:
    # [{'status': 'ok'} or {'status': 'ok', 'value': xxx} or
    # {'status': 'fail', 'reason': 'timeout' or 'abort' or 'not_found'}].
    # Elements of this list can be processed with process_result_read() and
    # process_result_write().
    def req_list(reqlist)
      result = @conn.call(:req_list_commit_each, [reqlist.get_requests()])
      @conn.class.process_result_req_list_tso(result)
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
      # note: we need to process a commit result as the write has been committed
      @conn.class.check_fail_abort(result)
      @conn.class.process_result_commit(result)
    end

    # Processes a result element from the list returned by req_list() which
    # originated from a add_del_on_list operation.
    # Raises the appropriate exceptions if a failure occurred during the
    # operation.
    def process_result_add_del_on_list(result)
      @conn.class.check_fail_abort(result)
      @conn.class.process_result_add_del_on_list(result)
    end

    # Processes a result element from the list returned by req_list() which
    # originated from a add_on_nr operation.
    # Raises the appropriate exceptions if a failure occurred during the
    # operation.
    def process_result_add_on_nr(result)
      @conn.class.check_fail_abort(result)
      @conn.class.process_result_add_on_nr(result)
    end

    # Processes a result element from the list returned by req_list() which
    # originated from a test_and_set operation.
    # Raises the appropriate exceptions if a failure occurred during the
    # operation.
    def process_result_test_and_set(result)
      @conn.class.check_fail_abort(result)
      @conn.class.process_result_test_and_set(result)
    end

    # Read the value at key.
    # Beware: lists of (small) integers may be (falsely) returned as a string -
    # use str_to_list() to convert such strings.
    def read(key)
      result = @conn.call(:read, [key])
      @conn.class.process_result_read(result)
    end

    # Write the value to key.
    def write(key, value, binary = false)
      value = @conn.class.encode_value(value, binary)
      result = @conn.call(:write, [key, value])
      @conn.class.check_fail_abort(result)
      @conn.class.process_result_commit(result)
    end

    # Changes the list stored at the given key, i.e. first adds all items in
    # to_add then removes all items in to_remove.
    # Both, to_add and to_remove, must be lists.
    # Assumes en empty list if no value exists at key.
    def add_del_on_list(key, to_add, to_remove)
      result = @conn.call(:add_del_on_list, [key, to_add, to_remove])
      @conn.class.check_fail_abort(result)
      @conn.class.process_result_add_del_on_list(result)
    end

    # Changes the number stored at the given key, i.e. adds some value.
    # Assumes 0 if no value exists at key.
    def add_on_nr(key, to_add)
      result = @conn.call(:add_on_nr, [key, to_add])
      @conn.class.check_fail_abort(result)
      @conn.class.process_result_add_on_nr(result)
    end

    # Atomic test and set, i.e. if the old value at key is old_value, then
    # write new_value.
    def test_and_set(key, old_value, new_value)
      result = @conn.call(:test_and_set, [key, old_value, new_value])
      @conn.class.check_fail_abort(result)
      @conn.class.process_result_test_and_set(result)
    end

    # Atomic test and set, i.e. if the old value at key is oldvalue, then
    # write newvalue.
    def test_and_set(key, oldvalue, newvalue)
      oldvalue = @conn.class.encode_value(oldvalue)
      newvalue = @conn.class.encode_value(newvalue)
      result = @conn.call(:test_and_set, [key, oldvalue, newvalue])
      @conn.class.check_fail_abort(result)
      @conn.class.process_result_test_and_set(result)
    end

    include InternalScalarisNopClose
  end
end
