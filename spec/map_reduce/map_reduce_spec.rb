require 'spec_helper'

describe "MapReduce stack" do
  it "should map and reduce some data" do
    pid = fork do
      EM.run do
        master = MapReduce::Master.new
        # master.run
      end
    end

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

    Process.kill "TERM", pid
  end
end