class TestPubSub < Minitest::Test
  def setup
    @testTime = (Time.now.to_f * 1000).to_i
  end

  # checks if there are more elements in list than in expectedElements and returns one of those elements
  def self._getDiffElement(list, expectedElements)
    expectedElements.each do |e|
      list.delete(e)
    end

    if list.length > 0
      return list[0]
    else
      return nil
    end
  end

  # Test method for PubSub()
  def test_pub_sub1()
    conn = Scalaris::PubSub.new()
    conn.close_connection()
  end

  # Test method for PubSub(conn)
  def test_pub_sub2()
    conn = Scalaris::PubSub.new(conn = Scalaris::JSONConnection.new(url = Scalaris::DEFAULT_URL))
    conn.close_connection()
  end

  # Test method for PubSub.close_connection() trying to close the connection twice.
  def test_double_close()
    conn = Scalaris::PubSub.new()
    conn.close_connection()
    conn.close_connection()
  end

  # Test method for PubSub.publish(topic, content) with a closed connection.
  def test_publish_not_connected()
    topic = "_Publish_NotConnected"
    conn = Scalaris::PubSub.new()
    conn.close_connection()
    #assert_raises( Scalaris::ConnectionError ) { conn.publish(@testTime.to_s + topic, $_TEST_DATA[0]) }
    conn.publish(@testTime.to_s + topic, $_TEST_DATA[0])
    conn.close_connection()
  end

  # Test method for PubSub.publish(topic, content).
  # Publishes some topics and uses a distinct key for each value.
  def test_publish1()
    topic = "_Publish1_"
    conn = Scalaris::PubSub.new()

    (0..($_TEST_DATA.length - 1)).each do |i|
      conn.publish(@testTime.to_s + topic + i.to_s, $_TEST_DATA[i])
    end

    conn.close_connection()
  end

  # Test method for PubSub.publish(topic, content).
  # Publishes some topics and uses a single key for all the values.
  def test_publish2()
    topic = "_Publish2"
    conn = Scalaris::PubSub.new()

    (0..($_TEST_DATA.length - 1)).each do |i|
      conn.publish(@testTime.to_s + topic, $_TEST_DATA[i])
    end

    conn.close_connection()
  end

  # Test method for PubSub.get_subscribers(topic) with a closed connection.
  def test_get_subscribers_otp_not_connected()
    topic = "_GetSubscribers_NotConnected"
    conn = Scalaris::PubSub.new()
    conn.close_connection()
    #assert_raises( Scalaris::ConnectionError ) { conn.get_subscribers(@testTime.to_s + topic) }
    conn.get_subscribers(@testTime.to_s + topic)
    conn.close_connection()
  end

  # Test method for PubSub.get_subscribers(topic).
  # Tries to get a subscriber list from an empty topic.
  def test_get_subscribers_not_existing_topic()
    topic = "_GetSubscribers_NotExistingTopic"
    conn = Scalaris::PubSub.new()
    subscribers = conn.get_subscribers(@testTime.to_s + topic)
    assert_equal([], subscribers)
    conn.close_connection()
  end

  # Test method for PubSub.subscribe(topic url) with a closed connection.
  def test_subscribe_not_connected()
    topic = "_Subscribe_NotConnected"
    conn = Scalaris::PubSub.new()
    conn.close_connection()
    #assert_raises( Scalaris::ConnectionError ) { conn.subscribe(@testTime.to_s + topic, $_TEST_DATA[0]) }
    conn.subscribe(@testTime.to_s + topic, $_TEST_DATA[0])
    conn.close_connection()
  end

  # Test method for PubSub.subscribe(topic, url) and PubSub.get_subscribers(topic).
  # Subscribes some arbitrary URLs to arbitrary topics and uses a distinct topic for each URL.
  def test_subscribe1()
    topic = "_Subscribe1_"
    conn = Scalaris::PubSub.new()

    (0..($_TEST_DATA.length - 1)).each do |i|
      conn.subscribe(@testTime.to_s + topic + i.to_s, $_TEST_DATA[i])
    end

    # check if the subscribers were successfully saved:
    (0..($_TEST_DATA.length - 1)).each do |i|
      topic1 = topic + i.to_s
      subscribers = conn.get_subscribers(@testTime.to_s + topic1)
      assert(subscribers.include?($_TEST_DATA[i]),
             "Subscriber \"" + $_TEST_DATA[i] + "\" does not exist for topic \"" + topic1 + "\"")
      assert_equal(1, subscribers.length,
                   "Subscribers of topic (" + topic1 + ") should only be [" + $_TEST_DATA[i] + "], but is: " + subscribers.to_s)
    end

    conn.close_connection()
  end

  # Test method for PubSub.subscribe(topic, url) and PubSub.get_subscribers(topic).
  # Subscribes some arbitrary URLs to arbitrary topics and uses a single topic for all URLs.
  def test_subscribe2()
    topic = "_Subscribe2"
    conn = Scalaris::PubSub.new()

    (0..($_TEST_DATA.length - 1)).each do |i|
      conn.subscribe(@testTime.to_s + topic, $_TEST_DATA[i])
    end

    # check if the subscribers were successfully saved:
    subscribers = conn.get_subscribers(@testTime.to_s + topic)
    (0..($_TEST_DATA.length - 1)).each do |i|
      assert(subscribers.include?($_TEST_DATA[i]),
             "Subscriber \"" + $_TEST_DATA[i] + "\" does not exist for topic \"" + topic + "\"")
    end
    assert_equal(nil, self.class._getDiffElement(subscribers, $_TEST_DATA),
                 "unexpected subscriber of topic \"" + topic + "\"")

    conn.close_connection()
  end

  # Test method for PubSub.unsubscribe(topic url) with a closed connection.
  def test_unsubscribe_not_connected()
    topic = "_Unsubscribe_NotConnected"
    conn = Scalaris::PubSub.new()
    conn.close_connection()
    #assert_raises( Scalaris::ConnectionError ) { conn.unsubscribe(@testTime.to_s + topic, $_TEST_DATA[0]) }
    assert_raises( Scalaris::NotFoundError ) { conn.unsubscribe(@testTime.to_s + topic, $_TEST_DATA[0]) }
    conn.close_connection()
  end

  # Test method for PubSub.unsubscribe(topic url) and PubSub.get_subscribers(topic).
  # Tries to unsubscribe an URL from a non-existing topic and tries to get the subscriber list afterwards.
  def test_unsubscribe_not_existing_topic()
    topic = "_Unsubscribe_NotExistingTopic"
    conn = Scalaris::PubSub.new()
    # unsubscribe test "url":
    assert_raises( Scalaris::NotFoundError ) { conn.unsubscribe(@testTime.to_s + topic, $_TEST_DATA[0]) }

    # check whether the unsubscribed urls were unsubscribed:
    subscribers = conn.get_subscribers(@testTime.to_s + topic)
    assert(!(subscribers.include?($_TEST_DATA[0])),
           "Subscriber \"" + $_TEST_DATA[0] + "\" should have been unsubscribed from topic \"" + topic + "\"")
    assert_equal(0, subscribers.length,
                 "Subscribers of topic (" + topic + ") should only be [], but is: " + subscribers.to_s)

    conn.close_connection()
  end

  # Test method for PubSub.subscribe(topic url), PubSub.unsubscribe(topic url) and PubSub.get_subscribers(topic).
  # Tries to unsubscribe an unsubscribed URL from an existing topic and compares the subscriber list afterwards.
  def test_unsubscribe_not_existing_url()
    topic = "_Unsubscribe_NotExistingUrl"
    conn = Scalaris::PubSub.new()

    # first subscribe test "urls"...
    conn.subscribe(@testTime.to_s + topic, $_TEST_DATA[0])
    conn.subscribe(@testTime.to_s + topic, $_TEST_DATA[1])

    # then unsubscribe another "url":
    assert_raises( Scalaris::NotFoundError ) { conn.unsubscribe(@testTime.to_s + topic, $_TEST_DATA[2]) }

    # check whether the subscribers were successfully saved:
    subscribers = conn.get_subscribers(@testTime.to_s + topic)
    assert(subscribers.include?($_TEST_DATA[0]),
           "Subscriber \"" + $_TEST_DATA[0] + "\" does not exist for topic \"" + topic + "\"")
    assert(subscribers.include?($_TEST_DATA[1]),
           "Subscriber \"" + $_TEST_DATA[1] + "\" does not exist for topic \"" + topic + "\"")

    # check whether the unsubscribed urls were unsubscribed:
    assert(!(subscribers.include?($_TEST_DATA[2])),
           "Subscriber \"" + $_TEST_DATA[2] + "\" should have been unsubscribed from topic \"" + topic + "\"")

    assert_equal(2, subscribers.length,
                 "Subscribers of topic (" + topic + ") should only be [\"" + $_TEST_DATA[0] + "\", \"" + $_TEST_DATA[1] + "\"], but is: " + subscribers.to_s)

    conn.close_connection()
  end

  # Test method for PubSub.subscribe(topic url), PubSub.unsubscribe(topic url) and PubSub.get_subscribers(topic).
  # Subscribes some arbitrary URLs to arbitrary topics and uses a distinct topic for each URL.
  # Unsubscribes every second subscribed URL.
  def test_unsubscribe1()
    topic = "_UnsubscribeString1_"
    conn = Scalaris::PubSub.new()

    # first subscribe test "urls"...
    (0..($_TEST_DATA.length - 1)).each do |i|
      conn.subscribe(@testTime.to_s + topic + i.to_s, $_TEST_DATA[i])
    end

    # ... then unsubscribe every second url:
    (0..($_TEST_DATA.length - 1)).step(2) do |i|
      conn.unsubscribe(@testTime.to_s + topic + i.to_s, $_TEST_DATA[i])
    end

    # check whether the subscribers were successfully saved:
    (1..($_TEST_DATA.length - 1)).step(2) do |i|
      topic1 = topic + i.to_s
      subscribers = conn.get_subscribers(@testTime.to_s + topic1)
      assert(subscribers.include?($_TEST_DATA[i]),
             "Subscriber \"" + $_TEST_DATA[i] + "\" does not exist for topic \"" + topic1 + "\"")
      assert_equal(1, subscribers.length,
                   "Subscribers of topic (" + topic1 + ") should only be [\"" + $_TEST_DATA[i] + "\"], but is: " + subscribers.to_s)
    end

    # check whether the unsubscribed urls were unsubscribed:
    (0..($_TEST_DATA.length - 1)).step(2) do |i|
      topic1 = topic + i.to_s
      subscribers = conn.get_subscribers(@testTime.to_s + topic1)
      assert(!(subscribers.include?($_TEST_DATA[i])),
             "Subscriber \"" + $_TEST_DATA[i] + "\" should have been unsubscribed from topic \"" + topic1 + "\"")
      assert_equal(0, subscribers.length,
                   "Subscribers of topic (" + topic1 + ") should only be [], but is: " + subscribers.to_s)
    end

    conn.close_connection()
  end

  # Test method for PubSub.subscribe(topic url), PubSub.unsubscribe(topic url) and PubSub.get_subscribers(topic).
  # Subscribes some arbitrary URLs to arbitrary topics and uses a single topic for all URLs.
  # Unsubscribes every second subscribed URL.
  def test_unsubscribe2()
    topic = "_UnubscribeString2"
    conn = Scalaris::PubSub.new()

    # first subscribe all test "urls"...
    (0..($_TEST_DATA.length - 1)).each do |i|
      conn.subscribe(@testTime.to_s + topic, $_TEST_DATA[i])
    end

    # ... then unsubscribe every second url:
    (0..($_TEST_DATA.length - 1)).step(2) do |i|
      conn.unsubscribe(@testTime.to_s + topic, $_TEST_DATA[i])
    end

    # check whether the subscribers were successfully saved:
    subscribers = conn.get_subscribers(@testTime.to_s + topic)
    subscribers_expected = []
    (1..($_TEST_DATA.length - 1)).step(2) do |i|
      subscribers_expected << $_TEST_DATA[i]
      assert(subscribers.include?($_TEST_DATA[i]),
             "Subscriber \"" + $_TEST_DATA[i] + "\" does not exist for topic \"" + topic + "\"")
    end

    # check whether the unsubscribed urls were unsubscribed:
    (0..($_TEST_DATA.length - 1)).step(2) do |i|
      assert(!(subscribers.include?($_TEST_DATA[i])),
             "Subscriber \"" + $_TEST_DATA[i] + "\" should have been unsubscribed from topic \"" + topic + "\"")
    end

    assert_equal(nil, self.class._getDiffElement(subscribers, subscribers_expected),
                 "unexpected subscriber of topic \"" + topic + "\"")

    conn.close_connection()
  end

  # Test method for PubSub.write(key, value=bytearray()) with a
  # request that is too large.
  def test_req_too_large()
      conn = Scalaris::PubSub.new()
      data = (0..($_TOO_LARGE_REQUEST_SIZE)).map{0}.join()
      key = "_ReqTooLarge"
      begin
        conn.publish(@testTime.to_s + key, data)
        assert(false, 'The publish should have failed unless yaws_max_post_data was set larger than ' + $_TOO_LARGE_REQUEST_SIZE.to_s())
      rescue Scalaris::ConnectionError
      end

      conn.close_connection()
  end
end
