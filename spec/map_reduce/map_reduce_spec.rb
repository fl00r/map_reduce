require 'spec_helper'

describe "MapReduce stack" do
  describe "single master" do
    before do
      @pid1 = fork do
        master = MapReduce::Master.new socket: "tcp://127.0.0.1:15555", 
                                       log_folder: "/tmp/map_reduce/master1"
        trap("SIGINT") do
          master.stop
          exit
        end
        master.run
      end
      @pid2 = fork do
        master = MapReduce::Master.new socket: "tcp://127.0.0.1:15556", 
                                       log_folder: "/tmp/map_reduce/master2"
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
      it "should map/reduce with multiple masters" do
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
      it "should map/reduce with multiple masters" do
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

      it "should map -> reduce / reduce" do
        EM.synchrony do
          @mapper = MapReduce::Mapper.new type: :sync, task: "Fruits", masters: ["tcp://127.0.0.1:15555", "tcp://127.0.0.1:15556"]
          @reducer = MapReduce::Reducer.new type: :sync, task: "Fruits", masters: ["tcp://127.0.0.1:15555", "tcp://127.0.0.1:15556"]

          Fiber.new do
            100.times do |i|
              @mapper.map(i, 1)
            end
          end.resume
          data = []
          Fiber.new do
            while data.size < 100
              @reducer.reduce do |k, v|
                data << k
              end
            end
            data.sort.must_equal (0...100).to_a.map(&:to_s).sort

            EM.stop
          end.resume
        end
      end

      it "should map/reduce-map/reduce with multiple masters" do
        EM.synchrony do
          @mapper1 = MapReduce::Mapper.new type: :sync, task: "Fruits", masters: ["tcp://127.0.0.1:15555", "tcp://127.0.0.1:15556"]
          @reducer1 = MapReduce::Reducer.new type: :sync, task: "Fruits", masters: ["tcp://127.0.0.1:15555", "tcp://127.0.0.1:15556"]
          @mapper2 = MapReduce::Mapper.new type: :sync, task: "Related", masters: ["tcp://127.0.0.1:15555", "tcp://127.0.0.1:15556"]
          @reducer2 = MapReduce::Reducer.new type: :sync, task: "Related", masters: ["tcp://127.0.0.1:15555", "tcp://127.0.0.1:15556"]

          [["Peter", "Apple"], ["Andrew", "Peach"], ["Mary", "Plum"], ["Peter", "Lemon"], ["Andrew", "Orange"], ["Peter", "Peach"], ["Yura", "Peach"], ["Yura", "Apricot"], ["Yura", "Apple"]].each do |a|
            res = @mapper1.map(*a)
            res.must_equal ["ok"]
          end

          @reducer1.reduce do |k, values|
            values.each do |fruit|
              related = values.dup
              related.delete fruit
              related.each do |r|
                @mapper2.map(fruit, r)
              end
            end
          end

          fruits = {}
          @reducer2.reduce do |fruit, related|
            fruits[fruit] ||= []
            fruits[fruit].push(*related)
          end

          fruits["Apple"].must_equal ["Apricot", "Lemon", "Peach", "Peach"]
          fruits["Orange"].must_equal ["Peach"]
          fruits["Plum"].must_equal nil

          EM.stop
        end
      end

      it "should use one master" do
        EM.synchrony do
          # MapReduce::Reducer.const_set :TIMEOUT, 0.1
          Process.kill "INT", @pid2
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