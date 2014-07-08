module Scalaroid
  # Request list for use with Transaction.req_list().
  class JSONReqListTransaction < JSONReqList
    def initialize(other = nil)
      super(other)
    end
  end
end
