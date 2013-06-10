require 'spec_helper'

describe "MapReduce stack" do
  describe "single master" do
    before do
      @pid = fork do
        master = MapReduce::Master.new
        master.run
      end
    end

    after do
      Process.kill "TERM", @pid
    end

    describe ":em" do
      it "should map some data" do
        EM.run do
          @mapper = MapReduce::Mapper.new task: "Fruits"
          @reducer = MapReduce::Reducer.new task: "Fruits"
          @mapper.map("Peter", "Apple") do |res|
            res.must_equal ["ok"]
            @mapper.map("Peter", "Apple") do |res|
              res.must_equal ["ok"]
              data = {}
              @reducer.reduce do |key, values|
                if key
                  data[key] = values
                else
                  data.size.must_equal 1
                  data.keys.must_equal ["Peter"]
                  data.values.must_equal [["Apple", "Apple"]]
                  EM.stop
                end
              end
            end
          end
        end
      end
    end

    describe ":sync" do
      it "should map some data" do
        EM.synchrony do
          @mapper = MapReduce::Mapper.new type: :sync, task: "Fruits"
          res = @mapper.map("Peter", "Apple")
          res.must_equal ["ok"]
          res = @mapper.map("Peter", "Lemon")
          res.must_equal ["ok"]
          EM.stop
        end
      end
    end
  end
end