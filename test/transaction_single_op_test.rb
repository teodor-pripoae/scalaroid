require_relative "test_helper"

class TestTransactionSingleOp < Minitest::Test
  def setup
    @testTime = (Time.now.to_f * 1000).to_i
  end

  # Test method for TransactionSingleOp()
  def test_transaction_single_op1()
    conn = Scalaroid::TransactionSingleOp.new()
    conn.close_connection()
  end

  # Test method for TransactionSingleOp(conn)
  def test_transaction_single_op2()
    conn = Scalaroid::TransactionSingleOp.new(conn = Scalaroid::JSONConnection.new(url = Scalaroid::DEFAULT_URL))
    conn.close_connection()
  end

  # Test method for TransactionSingleOp.close_connection() trying to close the connection twice.
  def test_double_close()
    conn = Scalaroid::TransactionSingleOp.new()
    conn.close_connection()
    conn.close_connection()
  end

  # Test method for TransactionSingleOp.read(key)
  def test_read_NotFound()
    key = "_Read_NotFound"
    conn = Scalaroid::TransactionSingleOp.new()
    assert_raises( Scalaroid::NotFoundError ) { conn.read(@testTime.to_s + key) }
    conn.close_connection()
  end

  # Test method for TransactionSingleOp.read(key) with a closed connection.
  def test_read_not_connected()
    key = "_Read_NotConnected"
    conn = Scalaroid::TransactionSingleOp.new()
    conn.close_connection()
    #assert_raises( Scalaroid::ConnectionError ) { conn.read(@testTime.to_s + key) }
    assert_raises( Scalaroid::NotFoundError ) { conn.read(@testTime.to_s + key) }
    conn.close_connection()
  end

  # Test method for TransactionSingleOp.write(key, value=str()) with a closed connection.
  def test_write_string_not_connected()
    key = "_WriteString_NotConnected"
    conn = Scalaroid::TransactionSingleOp.new()
    conn.close_connection()
    #assert_raises( Scalaroid::ConnectionError ) { conn.write(@testTime.to_s + key, $_TEST_DATA[0]) }
    conn.write(@testTime.to_s + key, $_TEST_DATA[0])
    conn.close_connection()
  end

  # Test method for TransactionSingleOp.write(key, value=str()) and TransactionSingleOp.read(key).
  # Writes strings and uses a distinct key for each value. Tries to read the data afterwards.
  def test_write_string1()
    key = "_WriteString1_"
    conn = Scalaroid::TransactionSingleOp.new()

    (0..($_TEST_DATA.length - 1)).each do |i|
      conn.write(@testTime.to_s + key + i.to_s, $_TEST_DATA[i])
    end

    # now try to read the data:
    (0..($_TEST_DATA.length - 1)).each do |i|
      actual = conn.read(@testTime.to_s + key + i.to_s)
      assert_equal($_TEST_DATA[i], actual)
    end

    conn.close_connection()
  end

  # Test method for TransactionSingleOp.write(key, value=str()) and TransactionSingleOp.read(key).
  # Writes strings and uses a single key for all the values. Tries to read the data afterwards.
  def test_write_string2()
    key = "_WriteString2"
    conn = Scalaroid::TransactionSingleOp.new()

    (0..($_TEST_DATA.length - 1)).each do |i|
      conn.write(@testTime.to_s + key.to_s, $_TEST_DATA[i])
    end

    # now try to read the data:
    actual = conn.read(@testTime.to_s + key.to_s)
    assert_equal($_TEST_DATA[$_TEST_DATA.length - 1], actual)
    conn.close_connection()
  end

  # Test method for TransactionSingleOp.write(key, value=list()) with a closed connection.
  def test_write_list_not_connected()
    key = "_WriteList_NotConnected"
    conn = Scalaroid::TransactionSingleOp.new()
    conn.close_connection()
    #assert_raises( Scalaroid::ConnectionError ) { conn.write(@testTime.to_s + key, [$_TEST_DATA[0], $_TEST_DATA[1]]) }
    conn.write(@testTime.to_s + key, [$_TEST_DATA[0], $_TEST_DATA[1]])
    conn.close_connection()
  end

  # Test method for TransactionSingleOp.write(key, value=list()) and TransactionSingleOp.read(key).
  # Writes strings and uses a distinct key for each value. Tries to read the data afterwards.
  def test_write_list1()
    key = "_WriteList1_"
    conn = Scalaroid::TransactionSingleOp.new()

    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      conn.write(@testTime.to_s + key + i.to_s, [$_TEST_DATA[i], $_TEST_DATA[i + 1]])
    end

    # now try to read the data:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      actual = conn.read(@testTime.to_s + key + i.to_s)
      assert_equal([$_TEST_DATA[i], $_TEST_DATA[i + 1]], actual)
    end

    conn.close_connection()
  end

  # Test method for TransactionSingleOp.write(key, value=list()) and TransactionSingleOp.read(key).
  # Writes strings and uses a single key for all the values. Tries to read the data afterwards.
  def test_write_list2()
    key = "_WriteList2"
    conn = Scalaroid::TransactionSingleOp.new()

    list = []
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      list = [$_TEST_DATA[i], $_TEST_DATA[i + 1]]
      conn.write(@testTime.to_s + key, list)
    end

    # now try to read the data:
    actual = conn.read(@testTime.to_s + key)
    assert_equal(list, actual)
    conn.close_connection()
  end

  # Test method for TransactionSingleOp.test_and_set(key, oldvalue=str(), newvalue=str()) with a closed connection.
  def test_test_and_set_string_not_connected()
    key = "_TestAndSetString_NotConnected"
    conn = Scalaroid::TransactionSingleOp.new()
    conn.close_connection()
    #assert_raises( Scalaroid::ConnectionError ) { conn.test_and_set(@testTime.to_s + key, $_TEST_DATA[0], $_TEST_DATA[1]) }
    assert_raises( Scalaroid::NotFoundError ) { conn.test_and_set(@testTime.to_s + key, $_TEST_DATA[0], $_TEST_DATA[1]) }
    conn.close_connection()
  end

  # Test method for TransactionSingleOp.test_and_set(key, oldvalue=str(), newvalue=str()).
  # Tries test_and_set with a non-existing key.
  def test_test_and_set_string_not_found()
    key = "_TestAndSetString_NotFound"
    conn = Scalaroid::TransactionSingleOp.new()
    assert_raises( Scalaroid::NotFoundError ) { conn.test_and_set(@testTime.to_s + key, $_TEST_DATA[0], $_TEST_DATA[1]) }
    conn.close_connection()
  end

  # Test method for TransactionSingleOp.test_and_set(key, oldvalue=str(), newvalue=str()),
  # TransactionSingleOp.read(key) and TransactionSingleOp.write(key, value=str()).
  # Writes a string and tries to overwrite it using test_and_set
  # knowing the correct old value. Tries to read the string afterwards.
  def test_test_and_set_string1()
    key = "_TestAndSetString1"
    conn = Scalaroid::TransactionSingleOp.new()

    # first write all values:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      conn.write(@testTime.to_s + key + i.to_s, $_TEST_DATA[i])
    end

    # now try to overwrite them using test_and_set:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      conn.test_and_set(@testTime.to_s + key + i.to_s, $_TEST_DATA[i], $_TEST_DATA[i + 1])
    end

    # now try to read the data:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      actual = conn.read(@testTime.to_s + key + i.to_s)
      assert_equal($_TEST_DATA[i + 1], actual)
    end

    conn.close_connection()
  end

  # Test method for TransactionSingleOp.test_and_set(key, oldvalue=str(), newvalue=str()),
  # TransactionSingleOp.read(key) and TransactionSingleOp.write(key, value=str()).
  # Writes a string and tries to overwrite it using test_and_set
  # knowing the wrong old value. Tries to read the string afterwards.
  def test_test_and_set_string2()
    key = "_TestAndSetString2"
    conn = Scalaroid::TransactionSingleOp.new()

    # first write all values:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      conn.write(@testTime.to_s + key + i.to_s, $_TEST_DATA[i])
    end

    # now try to overwrite them using test_and_set:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      begin
        conn.test_and_set(@testTime.to_s + key + i.to_s, $_TEST_DATA[i + 1], "fail")
        assert(false, 'expected a KeyChangedError')
      rescue Scalaroid::KeyChangedError => exception
        assert_equal($_TEST_DATA[i], exception.old_value)
      end
    end

    # now try to read the data:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      actual = conn.read(@testTime.to_s + key + i.to_s)
      assert_equal($_TEST_DATA[i], actual)
    end

    conn.close_connection()
  end

  # Test method for TransactionSingleOp.test_and_set(key, oldvalue=str(), newvalue=list()) with a closed connection.
  def test_test_and_set_list_not_connected()
    key = "_TestAndSetList_NotConnected"
    conn = Scalaroid::TransactionSingleOp.new()
    conn.close_connection()
    #assert_raises( Scalaroid::ConnectionError ) { conn.test_and_set(@testTime.to_s + key, "fail", [$_TEST_DATA[0], $_TEST_DATA[1]]) }
    assert_raises( Scalaroid::NotFoundError ) { conn.test_and_set(@testTime.to_s + key, "fail", [$_TEST_DATA[0], $_TEST_DATA[1]]) }
    conn.close_connection()
  end

  # Test method for TransactionSingleOp.test_and_set(key, oldvalue=str(), newvalue=list()).
  # Tries test_and_set with a non-existing key.
  def test_test_and_set_list_not_found()
    key = "_TestAndSetList_NotFound"
    conn = Scalaroid::TransactionSingleOp.new()
    assert_raises( Scalaroid::NotFoundError ) { conn.test_and_set(@testTime.to_s + key, "fail", [$_TEST_DATA[0], $_TEST_DATA[1]]) }
    conn.close_connection()
  end

  # Test method for TransactionSingleOp.test_and_set(key, oldvalue=str(), newvalue=list()),
  # TransactionSingleOp.read(key) and TransactionSingleOp.write(key, value=list()).
  # Writes a list and tries to overwrite it using test_and_set
  # knowing the correct old value. Tries to read the string afterwards.
  def test_test_and_set_list1()
    key = "_TestAndSetList1"
    conn = Scalaroid::TransactionSingleOp.new()

    # first write all values:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      conn.write(@testTime.to_s + key + i.to_s, [$_TEST_DATA[i], $_TEST_DATA[i + 1]])
    end

    # now try to overwrite them using test_and_set:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      conn.test_and_set(@testTime.to_s + key + i.to_s, [$_TEST_DATA[i], $_TEST_DATA[i + 1]], [$_TEST_DATA[i + 1], $_TEST_DATA[i]])
    end

    # now try to read the data:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      actual = conn.read(@testTime.to_s + key + i.to_s)
      assert_equal([$_TEST_DATA[i + 1], $_TEST_DATA[i]], actual)
    end

    conn.close_connection()
  end

  # Test method for TransactionSingleOp.test_and_set(key, oldvalue=str(), newvalue=list()),
  # TransactionSingleOp.read(key) and TransactionSingleOp.write(key, value=list()).
  # Writes a string and tries to overwrite it using test_and_set
  # knowing the wrong old value. Tries to read the string afterwards.
  def test_test_and_set_list2()
    key = "_TestAndSetList2"
    conn = Scalaroid::TransactionSingleOp.new()

    # first write all values:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      conn.write(@testTime.to_s + key + i.to_s, [$_TEST_DATA[i], $_TEST_DATA[i + 1]])
    end

    # now try to overwrite them using test_and_set:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      begin
        conn.test_and_set(@testTime.to_s + key + i.to_s, "fail", 1)
        assert(false, 'expected a KeyChangedError')
      rescue Scalaroid::KeyChangedError => exception
        assert_equal([$_TEST_DATA[i], $_TEST_DATA[i + 1]], exception.old_value)
      end
    end

    # now try to read the data:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      actual = conn.read(@testTime.to_s + key + i.to_s)
      assert_equal([$_TEST_DATA[i], $_TEST_DATA[i + 1]], actual)
    end

    conn.close_connection()
  end

  # Test method for TransactionSingleOp.req_list(RequestList) with an
  # empty request list.
  def test_req_list_empty()
    conn = Scalaroid::TransactionSingleOp.new()
    conn.req_list(conn.new_req_list())
    conn.close_connection()
  end

  # Test method for TransactionSingleOp.req_list(RequestList) with a
  # mixed request list.
  def test_req_list1()
    key = "_ReqList1_"
    conn = Scalaroid::TransactionSingleOp.new()

    readRequests = conn.new_req_list()
    firstWriteRequests = conn.new_req_list()
    writeRequests = conn.new_req_list()
    (0..($_TEST_DATA.length - 1)).each do |i|
      if (i % 2) == 0
        firstWriteRequests.add_write(@testTime.to_s + key + i.to_s, "first_" + $_TEST_DATA[i])
      end
      writeRequests.add_write(@testTime.to_s + key + i.to_s, "second_" + $_TEST_DATA[i])
      readRequests.add_read(@testTime.to_s + key + i.to_s)
    end

    results = conn.req_list(firstWriteRequests)
    # evaluate the first write results:
    (0..(firstWriteRequests.size() - 1)).step(2) do |i|
      conn.process_result_write(results[i])
    end

    results = conn.req_list(readRequests)
    assert_equal(readRequests.size(), results.length)
    # now evaluate the read results:
    (0..(readRequests.size() - 1)).step(2) do |i|
      if (i % 2) == 0
        actual = conn.process_result_read(results[i])
        assert_equal("first_" + $_TEST_DATA[i], actual)
      else
        begin
          conn.process_result_read(results[i])
          # a not found exception must be thrown
          assert(false, 'expected a NotFoundError')
        rescue Scalaroid::NotFoundError
        end
      end
    end

    results = conn.req_list(writeRequests)
    assert_equal(writeRequests.size(), results.length)
    # now evaluate the write results:
    (0..(writeRequests.size() - 1)).step(2) do |i|
      conn.process_result_write(results[i])
    end

    # once again test reads - now all reads should be successful
    results = conn.req_list(readRequests)
    assert_equal(readRequests.size(), results.length)

    # now evaluate the read results:
    (0..(readRequests.size() - 1)).step(2) do |i|
      actual = conn.process_result_read(results[i])
      assert_equal("second_" + $_TEST_DATA[i], actual)
    end

    conn.close_connection();
  end

  # Test method for TransactionSingleOp.write(key, value=bytearray()) with a
  # request that is too large.
  def test_req_too_large()
      conn = Scalaroid::TransactionSingleOp.new()
      data = (0..($_TOO_LARGE_REQUEST_SIZE)).map{0}.join()
      key = "_ReqTooLarge"
      begin
        conn.write(@testTime.to_s + key, data)
        assert(false, 'The write should have failed unless yaws_max_post_data was set larger than ' + $_TOO_LARGE_REQUEST_SIZE.to_s())
      rescue Scalaroid::ConnectionError
      end

      conn.close_connection()
  end
end
