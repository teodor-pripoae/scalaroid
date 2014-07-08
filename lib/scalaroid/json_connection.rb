module Scalaroid
  # Abstracts connections to Scalaris using JSON
  class JSONConnection
    # Creates a JSON connection to the given URL using the given TCP timeout (or default)
    def initialize(url = DEFAULT_URL, timeout = nil)
      begin
        @uri = URI.parse(url)
        @timeout = timeout
        start
      rescue Exception => error
        raise ConnectionError.new(error)
      end
    end

    def start
      if @conn == nil or not @conn.started?
        @conn = Net::HTTP.start(@uri.host, @uri.port)
        unless @timeout.nil?
          @conn.read_timeout = @timeout
        end
      end
    end
    private :start

    # Calls the given function with the given parameters via the JSON
    # interface of Scalaris.
    def call(function, params)
      start
      req = Net::HTTP::Post.new(DEFAULT_PATH)
      req.add_field('Content-Type', 'application/json; charset=utf-8')
      req.body = URI::encode({
        :jsonrpc => :'2.0',
        :method => function,
        :params => params,
        :id => 0 }.to_json({:ascii_only => true}))
      begin
        res = @conn.request(req)
        if res.is_a?(Net::HTTPSuccess)
          data = res.body
          return JSON.parse(data)['result']
        else
          raise ConnectionError.new(res)
        end
      rescue ConnectionError => error
        raise error
      rescue Exception => error
        raise ConnectionError.new(error)
      end
    end

    # Encodes the value to the form required by the Scalaris JSON API
    def self.encode_value(value, binary = false)
      if binary
        return { :type => :as_bin, :value => Base64.encode64(value) }
      else
        return { :type => :as_is, :value => value }
      end
    end

    # Decodes the value from the Scalaris JSON API form to a native type
    def self.decode_value(value)
      if not (value.has_key?('type') and value.has_key?('value'))
        raise ConnectionError.new(value)
      end
      if value['type'] == 'as_bin'
        return Base64.decode64(value['value'])
      else
        return value['value']
      end
    end

    # Processes the result of some Scalaris operation and raises a
    # TimeoutError if found.
    #
    # result: {'status': 'ok'} or
    #         {'status': 'fail', 'reason': 'timeout'}
    def self.check_fail_abort(result)
      if result == {:status => 'fail', :reason => 'timeout'}
        raise TimeoutError.new(result)
      end
    end

    # Processes the result of a read operation.
    # Returns the read value on success.
    # Raises the appropriate exception if the operation failed.
    #
    # result: {'status' => 'ok', 'value': xxx} or
    #         {'status' => 'fail', 'reason' => 'timeout' or 'not_found'}
    def self.process_result_read(result)
      if result.is_a?(Hash) and result.has_key?('status') and result.length == 2
        if result['status'] == 'ok' and result.has_key?('value')
          return decode_value(result['value'])
        elsif result['status'] == 'fail' and result.has_key?('reason')
          if result['reason'] == 'timeout'
            raise TimeoutError.new(result)
          elsif result['reason'] == 'not_found'
            raise NotFoundError.new(result)
          end
        end
      end
      raise UnknownError.new(result)
    end

    # Processes the result of a write operation.
    # Raises the appropriate exception if the operation failed.
    #
    # result: {'status' => 'ok'} or
    #         {'status' => 'fail', 'reason' => 'timeout'}
    def self.process_result_write(result)
      if result.is_a?(Hash)
        if result == {'status' => 'ok'}
          return true
        elsif result == {'status' => 'fail', 'reason' => 'timeout'}
          raise TimeoutError.new(result)
        end
      end
      raise UnknownError.new(result)
    end

    # Processes the result of a commit operation.
    # Raises the appropriate exception if the operation failed.
    #
    # result: {'status' => 'ok'} or
    #         {'status' => 'fail', 'reason' => 'abort', 'keys' => <list>} or
    #         {'status' => 'fail', 'reason' => 'timeout'}
    def self.process_result_commit(result)
      if result.is_a?(Hash) and result.has_key?('status')
        if result == {'status' => 'ok'}
          return true
        elsif result['status'] == 'fail' and result.has_key?('reason')
          if result.length == 2 and result['reason'] == 'timeout'
            raise TimeoutError.new(result)
          elsif result.length == 3 and result['reason'] == 'abort' and result.has_key?('keys')
            raise AbortError.new(result, result['keys'])
          end
        end
      end
      raise UnknownError.new(result)
    end

    # Processes the result of a add_del_on_list operation.
    # Raises the appropriate exception if the operation failed.
    #
    # results: {'status': 'ok'} or
    #          {'status': 'fail', 'reason': 'timeout' or 'not_a_list'}
    def self.process_result_add_del_on_list(result)
      if result.is_a?(Hash) and result.has_key?('status')
        if result == {'status' => 'ok'}
          return nil
        elsif result['status'] == 'fail' and result.has_key?('reason')
          if result.length == 2
            if result['reason'] == 'timeout'
              raise TimeoutError.new(result)
            elsif result['reason'] == 'not_a_list'
              raise NotAListError.new(result)
            end
          end
        end
      end
      raise UnknownError.new(result)
    end

    # Processes the result of a add_on_nr operation.
    # Raises the appropriate exception if the operation failed.
    #
    # results: {'status': 'ok'} or
    #          {'status': 'fail', 'reason': 'timeout' or 'not_a_number'}
    def self.process_result_add_on_nr(result)
      if result.is_a?(Hash) and result.has_key?('status')
        if result == {'status' => 'ok'}
          return nil
        elsif result['status'] == 'fail' and result.has_key?('reason')
          if result.length == 2
            if result['reason'] == 'timeout'
              raise TimeoutError.new(result)
            elsif result['reason'] == 'not_a_number'
              raise NotANumberError.new(result)
            end
          end
        end
      end
      raise UnknownError.new(result)
    end

    # Processes the result of a test_and_set operation.
    # Raises the appropriate exception if the operation failed.
    #
    # results: {'status' => 'ok'} or
    #          {'status' => 'fail', 'reason' => 'timeout' or 'not_found'} or
    #          {'status' => 'fail', 'reason' => 'key_changed', 'value': xxx}
    def self.process_result_test_and_set(result)
      if result.is_a?(Hash) and result.has_key?('status')
        if result == {'status' => 'ok'}
          return nil
        elsif result['status'] == 'fail' and result.has_key?('reason')
          if result.length == 2
            if result['reason'] == 'timeout'
              raise TimeoutError.new(result)
            elsif result['reason'] == 'not_found'
              raise NotFoundError.new(result)
            end
          elsif result['reason'] == 'key_changed' and result.has_key?('value') and result.length == 3
            raise KeyChangedError.new(result, decode_value(result['value']))
          end
        end
      end
      raise UnknownError.new(result)
    end

    # Processes the result of a publish operation.
    # Raises the appropriate exception if the operation failed.
    #
    # results: {'status': 'ok'}
    def self.process_result_publish(result)
      if result == {'status' => 'ok'}
        return nil
      end
      raise UnknownError.new(result)
    end

    # Processes the result of a subscribe operation.
    # Raises the appropriate exception if the operation failed.
    #
    # results: {'status': 'ok'} or
    #          {'status': 'fail', 'reason': 'timeout' or 'abort'}
    def self.process_result_subscribe(result)
      process_result_commit(result)
    end

    # Processes the result of a unsubscribe operation.
    # Raises the appropriate exception if the operation failed.
    #
    # results: {'status': 'ok'} or
    #          {'status': 'fail', 'reason': 'timeout' or 'abort' or 'not_found'}
    def self.process_result_unsubscribe(result)
      if result == {'status' => 'ok'}
        return nil
      elsif result.is_a?(Hash) and result.has_key?('status')
        if result['status'] == 'fail' and result.has_key?('reason')
          if result.length == 2
            if result['reason'] == 'timeout'
              raise TimeoutError.new(result)
            elsif result['reason'] == 'not_found'
              raise NotFoundError.new(result)
            end
          elsif result.length == 3 and result['reason'] == 'abort' and result.has_key?('keys')
            raise AbortError.new(result, result['keys'])
          end
        end
      end
      raise UnknownError.new(result)
    end

    # Processes the result of a get_subscribers operation.
    # Returns the list of subscribers on success.
    # Raises the appropriate exception if the operation failed.
    #
    # results: [urls=str()]
    def self.process_result_get_subscribers(result)
      if result.is_a?(Array)
        return result
      end
      raise UnknownError.new(result)
    end

    # Processes the result of a delete operation.
    # Returns an Array of
    # {:success => true | :timeout, :ok => <number of deleted items>, :results => <detailed results>}
    # on success.
    # Does not raise an exception if the operation failed unless the result
    # is invalid!
    #
    # results: {'ok': xxx, 'results': ['ok' or 'locks_set' or 'undef']} or
    #          {'failure': 'timeout', 'ok': xxx, 'results': ['ok' or 'locks_set' or 'undef']}
    def self.process_result_delete(result)
      if result.is_a?(Hash) and result.has_key?('ok') and result.has_key?('results')
        if not result.has_key?('failure')
          return {:success => true,
            :ok => result['ok'],
            :results => result['results']}
        elsif result['failure'] == 'timeout'
          return {:success => :timeout,
            :ok => result['ok'],
            :results => result['results']}
        end
      end
      raise UnknownError.new(result)
    end

    # Creates a new DeleteResult from the given result list.
    #
    # result: ['ok' or 'locks_set' or 'undef']
    def self.create_delete_result(result)
      ok = 0
      locks_set = 0
      undefined = 0
      if result.is_a?(Array)
        for element in result
          if element == 'ok'
              ok += 1
          elsif element == 'locks_set'
              locks_set += 1
          elsif element == 'undef'
              undefined += 1
          else
            raise UnknownError.new(:'Unknown reason ' + element + :'in ' + result)
          end
        end
        return DeleteResult.new(ok, locks_set, undefined)
      end
      raise UnknownError.new(:'Unknown result ' + result)
    end

    # Processes the result of a req_list operation of the Transaction class.
    # Returns the Array (:tlog => <tlog>, :result => <result>) on success.
    # Raises the appropriate exception if the operation failed.
    #
    # results: {'tlog': xxx,
    #           'results': [{'status': 'ok'} or {'status': 'ok', 'value': xxx} or
    #                       {'status': 'fail', 'reason': 'timeout' or 'abort' or 'not_found'}]}
    def self.process_result_req_list_t(result)
      if (not result.has_key?('tlog')) or (not result.has_key?('results')) or
          (not result['results'].is_a?(Array))
        raise UnknownError.new(result)
      end
      {:tlog => result['tlog'], :result => result['results']}
    end

    # Processes the result of a req_list operation of the TransactionSingleOp class.
    # Returns <result> on success.
    # Raises the appropriate exception if the operation failed.
    #
    # results: [{'status': 'ok'} or {'status': 'ok', 'value': xxx} or
    #           {'status': 'fail', 'reason': 'timeout' or 'abort' or 'not_found'}]
    def self.process_result_req_list_tso(result)
      if not result.is_a?(Array)
        raise UnknownError.new(result)
      end
      result
    end

    # Processes the result of a nop operation.
    # Raises the appropriate exception if the operation failed.
    #
    # result: 'ok'
    def self.process_result_nop(result)
      if result != 'ok'
        raise UnknownError.new(result)
      end
    end

    # Returns a new ReqList object allowing multiple parallel requests for
    # the Transaction class.
    def self.new_req_list_t(other = nil)
      JSONReqListTransaction.new(other)
    end

    # Returns a new ReqList object allowing multiple parallel requests for
    # the TransactionSingleOp class.
    def self.new_req_list_tso(other = nil)
      JSONReqListTransactionSingleOp.new(other)
    end

    def close
      if @conn.started?
        @conn.finish()
      end
    end
  end
end
