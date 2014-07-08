module Scalaroid
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
end
