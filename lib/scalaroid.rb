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
  autoload :ScalarisError, "scalaroid/errors"
  autoload :AbortError, "scalaroid/errors"
  autoload :ConnectionError, "scalaroid/errors"
  autoload :KeyChangedError, "scalaroid/errors"
  autoload :NodeNotFoundError, "scalaroid/errors"
  autoload :NotFoundError, "scalaroid/errors"
  autoload :NotAListError, "scalaroid/errors"
  autoload :NotANumberError, "scalaroid/errors"
  autoload :TimeoutError, "scalaroid/errors"
  autoload :UnknownError, "scalaroid/errors"

  autoload :Client, "scalaroid/client"
  autoload :DeleteResult, "scalaroid/delete_result"
  autoload :JSONConnection, "scalaroid/json_connection"
  autoload :JSONReqList, "scalaroid/json_req_list"
  autoload :JSONReqListTransaction, "scalaroid/json_req_list_transaction"
  autoload :JSONReqListTransactionSingleOp, "scalaroid/json_req_list_transaction_single_op"
  autoload :PubSub, "scalaroid/pub_sub"
  autoload :ReplicatedDHT, "scalaroid/replicated_dht"
  autoload :Transaction, "scalaroid/transaction"
  autoload :TransactionSingleOp, "scalaroid/transaction_single_op"
  autoload :VERSION, "scalaroid/version"

  # default URL and port to a scalaris node
  if ENV.has_key?('SCALARIS_JSON_URL') and not ENV['SCALARIS_JSON_URL'].empty?
    DEFAULT_URL = ENV['SCALARIS_JSON_URL']
  else
    DEFAULT_URL = 'http://localhost:8000'
  end

  # path to the json rpc page
  DEFAULT_PATH = '/jsonrpc.yaws'


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
