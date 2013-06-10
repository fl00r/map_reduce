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
          i = 0
          [["Peter", "Apple"], ["Andrew", "Peach"], ["Mary", "Plum"], ["Peter", "Lemon"], ["Andrew", "Orange"]].each do |a|
            @mapper.map(*a) do |res|
              res.must_equal ["ok"]
              if (i+=1) == 5
                data = {}
                @reducer.reduce do |key, values|
                  if key
                    data[key] = values
                  else
                    data.size.must_equal 3
                    data["Peter"].sort.must_equal ["Apple", "Lemon"].sort
                    data["Andrew"].sort.must_equal ["Peach", "Orange"].sort
                    data["Mary"].must_equal ["Plum"]
                    EM.stop
                  end
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
          @reducer = MapReduce::Reducer.new type: :sync, task: "Fruits"
          res = @mapper.map("Peter", "Apple")
          res = @mapper.map("Andrew", "Peach")
          res = @mapper.map("Mary", "Plum")
          res = @mapper.map("Peter", "Lemon")
          res = @mapper.map("Andrew", "Orange")
          res.must_equal ["ok"]
          data = {}
          @reducer.reduce do |k, values|
            data[k] = values
          end
          data.size.must_equal 3
          data["Peter"].sort.must_equal ["Apple", "Lemon"].sort
          data["Andrew"].sort.must_equal ["Peach", "Orange"].sort
          data["Mary"].must_equal ["Plum"]
          EM.stop
        end
      end
    end
  end
end