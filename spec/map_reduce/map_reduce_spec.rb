require 'spec_helper'

describe "MapReduce stack" do
  describe "single master" do
    before do
      @pid1 = fork do
        master = MapReduce::Master.new socket: "tcp://127.0.0.1:15555"
        trap("SIGINT") do
          master.stop
          exit
        end
        master.run
      end
      @pid2 = fork do
        master = MapReduce::Master.new socket: "tcp://127.0.0.1:15556"
        trap("SIGINT") do
          master.stop
          exit
        end
        master.run
      end
    end

    after do
      Process.kill "INT", @pid1
      Process.kill "INT", @pid2
    end

    describe ":em" do
      it "should map some data" do
        EM.run do
          @mapper = MapReduce::Mapper.new task: "Fruits", masters: ["tcp://127.0.0.1:15555", "tcp://127.0.0.1:15556"]
          @reducer = MapReduce::Reducer.new task: "Fruits", masters: ["tcp://127.0.0.1:15555", "tcp://127.0.0.1:15556"]
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
          @mapper = MapReduce::Mapper.new type: :sync, task: "Fruits", masters: ["tcp://127.0.0.1:15555", "tcp://127.0.0.1:15556"]
          @reducer = MapReduce::Reducer.new type: :sync, task: "Fruits", masters: ["tcp://127.0.0.1:15555", "tcp://127.0.0.1:15556"]
          [["Peter", "Apple"], ["Andrew", "Peach"], ["Mary", "Plum"], ["Peter", "Lemon"], ["Andrew", "Orange"]].each do |a|
            res = @mapper.map(*a)
            res.must_equal ["ok"]
          end
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