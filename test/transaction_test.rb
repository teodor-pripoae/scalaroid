require_relative "test_helper"

class TestTransaction < Minitest::Test
  def setup
    @testTime = (Time.now.to_f * 1000).to_i
  end

  # Test method for Transaction()
  def test_transaction1()
    t = Scalaroid::Transaction.new()
    t.close_connection()
  end

  # Test method for Transaction(conn)
  def test_transaction3()
    t = Scalaroid::Transaction.new(conn = Scalaroid::JSONConnection.new(url = Scalaroid::DEFAULT_URL))
    t.close_connection()
  end

  # Test method for Transaction.close_connection() trying to close the connection twice.
  def test_double_close()
    t = Scalaroid::Transaction.new()
    t.close_connection()
    t.close_connection()
  end

  # Test method for Transaction.commit() with a closed connection.
  def test_commit_not_connected()
    t = Scalaroid::Transaction.new()
    t.close_connection()
    #assert_raises( Scalaroid::ConnectionError ) { t.commit() }
    t.commit()
    t.close_connection()
  end

  # Test method for Transaction.commit() which commits an empty transaction.
  def test_commit_empty()
    t = Scalaroid::Transaction.new()
    t.commit()
    t.close_connection()
  end

  # Test method for Transaction.abort() with a closed connection.
  def test_abort_not_connected()
    t = Scalaroid::Transaction.new()
    t.close_connection()
    #assert_raises( Scalaroid::ConnectionError ) { t.abort() }
    t.abort()
    t.close_connection()
  end

  # Test method for Transaction.abort() which aborts an empty transaction.
  def test_abort_empty()
    t = Scalaroid::Transaction.new()
    t.abort()
    t.close_connection()
  end

  # Test method for Transaction.read(key)
  def test_read_not_found()
    key = "_Read_NotFound"
    t = Scalaroid::Transaction.new()
    assert_raises( Scalaroid::NotFoundError ) { t.read(@testTime.to_s + key) }
    t.close_connection()
  end

  # Test method for Transaction.read(key) with a closed connection.
  def test_read_not_connected()
    key = "_Read_NotConnected"
    t = Scalaroid::Transaction.new()
    t.close_connection()
    #assert_raises( Scalaroid::ConnectionError ) { t.read(@testTime.to_s + key) }
    assert_raises( Scalaroid::NotFoundError ) { t.read(@testTime.to_s + key) }
    t.close_connection()
  end

  # Test method for Transaction.write(key, value=str()) with a closed connection.
  def test_write_string_not_connected()
    key = "_WriteString_NotConnected"
    t = Scalaroid::Transaction.new()
    t.close_connection()
    #assert_raises( Scalaroid::ConnectionError ) { t.write(@testTime.to_s + key, $_TEST_DATA[0]) }
    t.write(@testTime.to_s + key, $_TEST_DATA[0])
    t.close_connection()
  end

  # Test method for Transaction.read(key) and Transaction.write(key, value=str())
  # which should show that writing a value for a key for which a previous read
  # returned a NotFoundError is possible.
  def test_write_string_not_found()
    key = "_WriteString_notFound"
    t = Scalaroid::Transaction.new()
    notFound = false
    begin
      t.read(@testTime.to_s + key)
    rescue Scalaroid::NotFoundError
      notFound = true
    end

    assert(notFound)
    t.write(@testTime.to_s + key, $_TEST_DATA[0])
    assert_equal($_TEST_DATA[0], t.read(@testTime.to_s + key))
    t.close_connection()
  end

  # Test method for Transaction.write(key, value=str()) and Transaction.read(key).
  # Writes strings and uses a distinct key for each value. Tries to read the data afterwards.
  def test_write_string()
    key = "_testWriteString1_"
    t = Scalaroid::Transaction.new()

    (0..($_TEST_DATA.length - 1)).each do |i|
      t.write(@testTime.to_s + key + i.to_s, $_TEST_DATA[i])
    end

    # now try to read the data:
    (0..($_TEST_DATA.length - 1)).each do |i|
      actual = t.read(@testTime.to_s + key + i.to_s)
      assert_equal($_TEST_DATA[i], actual)
    end

    # commit the transaction and try to read the data with a new one:
    t.commit()
    t = Scalaroid::Transaction.new()
    (0..($_TEST_DATA.length - 1)).each do |i|
      actual = t.read(@testTime.to_s + key + i.to_s)
      assert_equal($_TEST_DATA[i], actual)
    end

    t.close_connection()
  end

  # Test method for Transaction.write(key, value=list()) and Transaction.read(key).
  # Writes a list and uses a distinct key for each value. Tries to read the data afterwards.
  def test_write_list1()
    key = "_testWriteList1_"
    t = Scalaroid::Transaction.new()

    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      t.write(@testTime.to_s + key + i.to_s, [$_TEST_DATA[i], $_TEST_DATA[i + 1]])
    end

    # now try to read the data:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      actual = t.read(@testTime.to_s + key + i.to_s)
      assert_equal([$_TEST_DATA[i], $_TEST_DATA[i + 1]], actual)
    end

    t.close_connection()

    # commit the transaction and try to read the data with a new one:
    t.commit()
    t = Scalaroid::Transaction.new()
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      actual = t.read(@testTime.to_s + key + i.to_s)
      assert_equal([$_TEST_DATA[i], $_TEST_DATA[i + 1]], actual)
    end

    t.close_connection()
  end

  # Test method for Transaction.req_list(RequestList) with an
  # empty request list.
  def test_req_list_empty()
    conn = Scalaroid::Transaction.new()
    conn.req_list(conn.new_req_list())
    conn.close_connection()
  end

  # Test method for Transaction.req_list(RequestList) with a
  # mixed request list.
  def test_req_list1()
    key = "_ReqList1_"
    conn = Scalaroid::Transaction.new()

    readRequests = conn.new_req_list()
    firstWriteRequests = conn.new_req_list()
    writeRequests = conn.new_req_list()
    (0..($_TEST_DATA.length - 1)).each do |i|
      if (i % 2) == 0
        firstWriteRequests.add_write(@testTime.to_s + key + i.to_s, $_TEST_DATA[i])
      end
      writeRequests.add_write(@testTime.to_s + key + i.to_s, $_TEST_DATA[i])
      readRequests.add_read(@testTime.to_s + key + i.to_s)
    end

    results = conn.req_list(firstWriteRequests)
    # evaluate the first write results:
    (0..(firstWriteRequests.size() - 1)).each do |i|
      conn.process_result_write(results[i])
    end

    requests = conn.new_req_list(readRequests).concat(writeRequests).add_commit()
    results = conn.req_list(requests)
    assert_equal(requests.size(), results.length)

    # now evaluate the read results:
    (0..(readRequests.size() - 1)).each do |i|
      if (i % 2) == 0
        actual = conn.process_result_read(results[i])
        assert_equal($_TEST_DATA[i], actual)
      else
        begin
          conn.process_result_read(results[i])
          # a not found exception must be thrown
          assert(false, 'expected a NotFoundError')
        rescue Scalaroid::NotFoundError
        end
      end
    end

    # now evaluate the write results:
    (0..(writeRequests.size() - 1)).each do |i|
      pos = readRequests.size() + i
      conn.process_result_write(results[pos])
    end

    # once again test reads - now all reads should be successful
    results = conn.req_list(readRequests)
    assert_equal(readRequests.size(), results.length)

    # now evaluate the read results:
    (0..(readRequests.size() - 1)).each do |i|
      actual = conn.process_result_read(results[i])
      assert_equal($_TEST_DATA[i], actual)
    end

    conn.close_connection();
  end

  # Test method for Transaction.write(key, value=bytearray()) with a
  # request that is too large.
  def test_req_too_large()
      conn = Scalaroid::Transaction.new()
      data = (0..($_TOO_LARGE_REQUEST_SIZE)).map{0}.join()
      key = "_ReqTooLarge"
      begin
        conn.write(@testTime.to_s + key, data)
        assert(false, 'The write should have failed unless yaws_max_post_data was set larger than ' + $_TOO_LARGE_REQUEST_SIZE.to_s())
      rescue Scalaroid::ConnectionError
      end

      conn.close_connection()
  end

  # Various tests.
  def test_various()
      _writeSingleTest("_0:" + [0x0160].pack("U*") + "arplaninac:page_", $_TEST_DATA[0])
  end

  # Helper function for single write tests.
  # Writes a strings to some key and tries to read it afterwards.
  def _writeSingleTest(key, data)
    t = Scalaroid::Transaction.new()

    t.write(@testTime.to_s + key, data)
    # now try to read the data:
    assert_equal(data, t.read(@testTime.to_s + key))
    # commit the transaction and try to read the data with a new one:
    t.commit()
    t = Scalaroid::Transaction.new()
    assert_equal(data, t.read(@testTime.to_s + key))

    t.close_connection()
  end
  private :_writeSingleTest
end
