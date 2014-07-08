module Scalaroid
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
end
