require_relative "test_helper"

class TestClient < Minitest::Test
  def setup
    @testTime = (Time.now.to_f * 1000).to_i
    @client = Scalaroid::Client.new
  end

  def test_get
    assert_equal(@client.get(@testTime.to_s + "not_found"), nil)
  end

  def test_set
    key = @testTime.to_s + "key"
    value = @testTime.to_s + "value"
    assert_equal(true, @client.set(key, value))
  end
end
