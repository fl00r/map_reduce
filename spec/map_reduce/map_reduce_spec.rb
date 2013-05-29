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

    it "should map and reduce some data in CB mode" do
      EM.run do
        data = {}
        worker = MapReduce::Worker.new
        worker.map("Petr", ["Radiohead", "Muse", "R.E.M."] * ',') do
          worker.map("Alex", ["Madonna", "Lady Gaga"] * ',') do
            worker.map("Petr", ["Radiohead", "The Beatles", "Aquarium"] * ',') do
              worker.map_finished do
                worker.reduce do |key, values|
                  if key
                    data[key] = values
                  else
                    data.size.must_equal 2
                    data["Petr"].must_equal [["Radiohead", "Muse", "R.E.M."] * ',', ["Radiohead", "The Beatles", "Aquarium"] * ',']
                    data["Alex"].must_equal [["Madonna", "Lady Gaga"] * ',']

                    EM.stop
                  end
                end
              end
            end
          end
        end
      end
    end

    it "should map and reduce some data in SYNC mode" do
      EM.synchrony do
        data = {}
        worker = MapReduce::Worker.new type: :sync
        worker.map("Petr", ["Radiohead", "Muse", "R.E.M."] * ',')
        worker.map("Alex", ["Madonna", "Lady Gaga"] * ',')
        worker.map("Petr", ["Radiohead", "The Beatles", "Aquarium"] * ',')
        worker.map_finished
        worker.reduce do |key, values|
          data[key] = values  if key
        end
        data.size.must_equal 2
        data["Petr"].must_equal [["Radiohead", "Muse", "R.E.M."] * ',', ["Radiohead", "The Beatles", "Aquarium"] * ',']
        data["Alex"].must_equal [["Madonna", "Lady Gaga"] * ',']

        EM.stop
      end
    end
  end

  describe "multiple master" do
    before do
      @pid1 = fork do
        master = MapReduce::Master.new socket: "ipc:///dev/shm/sock1.sock"
        master.run
      end
      @pid2 = fork do
        master = MapReduce::Master.new socket: "ipc:///dev/shm/sock2.sock"
        master.run
      end
    end

    after do
      Process.kill "TERM", @pid1
      Process.kill "TERM", @pid2
    end

    it "should map and reduce some data in SYNC mode twice" do
      EM.synchrony do
        worker = MapReduce::Worker.new type: :sync,  masters: ["ipc:///dev/shm/sock1.sock", "ipc:///dev/shm/sock2.sock"]
        2.times do
          data = {}
          worker.map("Petr", ["Radiohead", "Muse", "R.E.M."] * ',')
          worker.map("Alex", ["Madonna", "Lady Gaga"] * ',')
          worker.map("Petr", ["Radiohead", "The Beatles", "Aquarium"] * ',')
          worker.map("Michael", ["Blur"] * ',')
          worker.map("Gosha", ["DDT", "Splin"] * ',')
          worker.map("Obama", ["Adele", "Rolling Stones"] * ',')
          worker.map_finished
          worker.reduce do |key, values|
            data[key] = values  if key
          end
          data.size.must_equal 5
          data["Petr"].must_equal [["Radiohead", "Muse", "R.E.M."] * ',', ["Radiohead", "The Beatles", "Aquarium"] * ',']
          data["Alex"].must_equal [["Madonna", "Lady Gaga"] * ',']
        end

        EM.stop
      end
    end
  end
end