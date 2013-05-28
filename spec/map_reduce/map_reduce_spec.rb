require 'spec_helper'

describe "MapReduce stack" do
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