module Scalaroid
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
end
