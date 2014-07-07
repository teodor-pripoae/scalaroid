require_relative "test_helper"

class TestReplicatedDHT < Minitest::Test
  def setup
    @testTime = (Time.now.to_f * 1000).to_i
  end

  # Test method for ReplicatedDHT()
  def test_replicated_dht1()
    rdht = Scalaris::ReplicatedDHT.new()
    rdht.close_connection()
  end

  # Test method for ReplicatedDHT(conn)
  def test_replicated_dht2()
    rdht = Scalaris::ReplicatedDHT.new(conn = Scalaris::JSONConnection.new(url = Scalaris::DEFAULT_URL))
    rdht.close_connection()
  end

  # Test method for ReplicatedDHT.close_connection() trying to close the connection twice.
  def test_double_close()
    rdht = Scalaris::ReplicatedDHT.new()
    rdht.close_connection()
    rdht.close_connection()
  end

  # Tries to read the value at the given key and fails if this does
  # not fail with a NotFoundError.
  def _checkKeyDoesNotExist(key)
    conn = Scalaris::TransactionSingleOp.new()
    begin
      conn.read(key)
      assert(false, 'the value at ' + key + ' should not exist anymore')
    rescue Scalaris::NotFoundError
      # nothing to do here
    end
    conn.close_connection()
  end

  # Test method for ReplicatedDHT.delete(key).
  # Tries to delete some not existing keys.
  def test_delete_not_existing_key()
    key = "_Delete_NotExistingKey"
    rdht = Scalaris::ReplicatedDHT.new()

    (0..($_TEST_DATA.length - 1)).each do |i|
      ok = rdht.delete(@testTime.to_s + key + i.to_s)
      assert_equal(0, ok)
      results = rdht.get_last_delete_result()
      assert_equal(0, results.ok)
      assert_equal(0, results.locks_set)
      assert_equal(4, results.undefined)
      _checkKeyDoesNotExist(@testTime.to_s + key + i.to_s)
    end

    rdht.close_connection()
  end

  # Test method for ReplicatedDHT.delete(key) and TransactionSingleOp#write(key, value=str()).
  # Inserts some values, tries to delete them afterwards and tries the delete again.
  def test_delete1()
    key = "_Delete1"
    c = Scalaris::JSONConnection.new(url = Scalaris::DEFAULT_URL)
    rdht = Scalaris::ReplicatedDHT.new(conn = c)
    sc = Scalaris::TransactionSingleOp.new(conn = c)

    (0..($_TEST_DATA.length - 1)).each do |i|
      sc.write(@testTime.to_s + key + i.to_s, $_TEST_DATA[i])
    end

    # now try to delete the data:
    (0..($_TEST_DATA.length - 1)).each do |i|
      ok = rdht.delete(@testTime.to_s + key + i.to_s)
      assert_equal(4, ok)
      results = rdht.get_last_delete_result()
      assert_equal(4, results.ok)
      assert_equal(0, results.locks_set)
      assert_equal(0, results.undefined)
      _checkKeyDoesNotExist(@testTime.to_s + key + i.to_s)

      # try again (should be successful with 0 deletes)
      ok = rdht.delete(@testTime.to_s + key + i.to_s)
      assert_equal(0, ok)
      results = rdht.get_last_delete_result()
      assert_equal(0, results.ok)
      assert_equal(0, results.locks_set)
      assert_equal(4, results.undefined)
      _checkKeyDoesNotExist(@testTime.to_s + key + i.to_s)
    end

    c.close()
  end

  # Test method for ReplicatedDHT.delete(key) and TransactionSingleOp#write(key, value=str()).
  # Inserts some values, tries to delete them afterwards, inserts them again and tries to delete them again (twice).
  def test_delete2()
    key = "_Delete2"
    c = Scalaris::JSONConnection.new(url = Scalaris::DEFAULT_URL)
    rdht = Scalaris::ReplicatedDHT.new(conn = c)
    sc = Scalaris::TransactionSingleOp.new(conn = c)

    (0..($_TEST_DATA.length - 1)).each do |i|
      sc.write(@testTime.to_s + key + i.to_s, $_TEST_DATA[i])
    end

    # now try to delete the data:
    (0..($_TEST_DATA.length - 1)).each do |i|
      ok = rdht.delete(@testTime.to_s + key + i.to_s)
      assert_equal(4, ok)
      results = rdht.get_last_delete_result()
      assert_equal(4, results.ok)
      assert_equal(0, results.locks_set)
      assert_equal(0, results.undefined)
      _checkKeyDoesNotExist(@testTime.to_s + key + i.to_s)
    end

    (0..($_TEST_DATA.length - 1)).each do |i|
      sc.write(@testTime.to_s + key + i.to_s, $_TEST_DATA[i])
    end

    # now try to delete the data:
    (0..($_TEST_DATA.length - 1)).each do |i|
      ok = rdht.delete(@testTime.to_s + key + i.to_s)
      assert_equal(4, ok)
      results = rdht.get_last_delete_result()
      assert_equal(4, results.ok)
      assert_equal(0, results.locks_set)
      assert_equal(0, results.undefined)
      _checkKeyDoesNotExist(@testTime.to_s + key + i.to_s)

      # try again (should be successful with 0 deletes)
      ok = rdht.delete(@testTime.to_s + key + i.to_s)
      assert_equal(0, ok)
      results = rdht.get_last_delete_result()
      assert_equal(0, results.ok)
      assert_equal(0, results.locks_set)
      assert_equal(4, results.undefined)
      _checkKeyDoesNotExist(@testTime.to_s + key + i.to_s)
    end

    c.close()
  end
end
