#!/usr/bin/ruby -KU
# Copyright 2008-2011 Zuse Institute Berlin
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

require 'rubygems'
gem 'json', '>=1.4.1'
require 'json'
require 'net/http'
require 'base64'
require 'open-uri'

module InternalScalarisSimpleError
  attr_reader :raw_result
  def initialize(raw_result)
    @raw_result = raw_result
  end

  def to_s
    @raw_result
  end
end

module InternalScalarisNopClose
  # No operation (may be used for measuring the JSON overhead).
  def nop(value)
    value = @conn.class.encode_value(value)
    result = @conn.call(:nop, [value])
    @conn.class.process_result_nop(result)
  end

  # Close the connection to Scalaris
  # (it will automatically be re-opened on the next request).
  def close_connection
    @conn.close()
  end
end

# work around floating point numbers not being printed precisely enough
class Float
  # note: can not override to_json (this is not done recursively, e.g. in a Hash, before ruby 1.9)
  alias_method :orig_t_s, :to_s
  def to_s
    if not finite?
      orig_to_json(*a)
    else
      sprintf("%#.17g", self)
    end
  end
end

module Scalaroid
  autoload :JSONConnection, "scalaroid/json_connection"
  autoload :VERSION, "scalaroid/version"

  # default URL and port to a scalaris node
  if ENV.has_key?('SCALARIS_JSON_URL') and not ENV['SCALARIS_JSON_URL'].empty?
    DEFAULT_URL = ENV['SCALARIS_JSON_URL']
  else
    DEFAULT_URL = 'http://localhost:8000'
  end

  # path to the json rpc page
  DEFAULT_PATH = '/jsonrpc.yaws'

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

  # Request list for use with Transaction.req_list()
  class JSONReqList
    # Create a new object using a JSON connection.
    def initialize(other = nil)
      @requests = []
      @is_commit = false
      if not other == nil
        concat(other)
      end
    end

    # Adds a read operation to the request list.
    def add_read(key)
      if (@is_commit)
          raise RuntimeError.new('No further request supported after a commit!')
      end
      @requests << {'read' => key}
      self
    end

    # Adds a write operation to the request list.
    def add_write(key, value, binary = false)
      if (@is_commit)
          raise RuntimeError.new('No further request supported after a commit!')
      end
      @requests << {'write' => {key => JSONConnection.encode_value(value, binary)}}
      self
    end

    # Adds a add_del_on_list operation to the request list.
    def add_add_del_on_list(key, to_add, to_remove)
      if (@is_commit)
          raise RuntimeError.new('No further request supported after a commit!')
      end
      @requests << {'add_del_on_list' => {'key' => key, 'add' => to_add, 'del'=> to_remove}}
      self
    end

    # Adds a add_on_nr operation to the request list.
    def add_add_on_nr(key, to_add)
      if (@is_commit)
          raise RuntimeError.new('No further request supported after a commit!')
      end
      @requests << {'add_on_nr' => {key => to_add}}
      self
    end

    # Adds a test_and_set operation to the request list.
    def add_test_and_set(key, old_value, new_value)
      if (@is_commit)
          raise RuntimeError.new('No further request supported after a commit!')
      end
      @requests << {'test_and_set' => {'key' => key,
          'old' => JSONConnection.encode_value(old_value, false),
          'new' => JSONConnection.encode_value(new_value, false)}}
      self
    end

    # Adds a commit operation to the request list.
    def add_commit
      if (@is_commit)
          raise RuntimeError.new('Only one commit per request list allowed!')
      end
      @requests << {'commit' => ''}
      @is_commit = true
      self
    end

    # Gets the collected requests.
    def get_requests
      @requests
    end

    # Returns whether the transactions contains a commit or not.
    def is_commit()
      @is_commit
    end

    # Checks whether the request list is empty.
    def is_empty()
      @requests.empty?
    end

    # Gets the number of requests in the list.
    def size()
      @requests.length
    end

    # Adds all requests of the other request list to the end of this list.
    def concat(other)
      @requests.concat(other.get_requests())
      self
    end
  end

  # Request list for use with Transaction.req_list().
  class JSONReqListTransaction < JSONReqList
    def initialize(other = nil)
      super(other)
    end
  end

  # Request list for use with TransactionSingleOp.req_list() which does not
  # support commits.
  class JSONReqListTransactionSingleOp < JSONReqList
    def initialize(other = nil)
      super(other)
    end

    # Adds a commit operation to the request list.
    def add_commit()
      raise RuntimeError.new('No commit allowed in TransactionSingleOp.req_list()!')
    end
  end

  # Publish and subscribe methods accessing Scalaris' pubsub system
  class PubSub
    # Create a new object using the given connection.
    def initialize(conn = JSONConnection.new())
      @conn = conn
    end

    # Publishes content under topic.
    def publish(topic, content)
      # note: do NOT encode the content, this is not decoded on the erlang side!
      # (only strings are allowed anyway)
      # content = @conn.class.encode_value(content)
      result = @conn.call(:publish, [topic, content])
      @conn.class.process_result_publish(result)
    end

    # Subscribes url for topic.
    def subscribe(topic, url)
      # note: do NOT encode the URL, this is not decoded on the erlang side!
      # (only strings are allowed anyway)
      # url = @conn.class.encode_value(url)
      result = @conn.call(:subscribe, [topic, url])
      @conn.class.process_result_subscribe(result)
    end

    # Unsubscribes url from topic.
    def unsubscribe(topic, url)
      # note: do NOT encode the URL, this is not decoded on the erlang side!
      # (only strings are allowed anyway)
      # url = @conn.class.encode_value(url)
      result = @conn.call(:unsubscribe, [topic, url])
      @conn.class.process_result_unsubscribe(result)
    end

    # Gets the list of all subscribers to topic.
    def get_subscribers(topic)
      result = @conn.call(:get_subscribers, [topic])
      @conn.class.process_result_get_subscribers(result)
    end

    include InternalScalarisNopClose
  end

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

  # Converts a string to a list of integers.
  # If the expected value of a read operation is a list, the returned value
  # could be (mistakenly) a string if it is a list of integers.
  def str_to_list(value)
    if value.is_a?(String)
      return value.unpack("U*")
    else
      return value
    end
  end

  module_function :str_to_list
end
