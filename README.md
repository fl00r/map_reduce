# MapReduce

MapReduce is a simple distributed MapReduce framework on Ruby.

Internally there are ZMQ Transport and Evenmachine.

## Installation

Add this line to your application's Gemfile:

    gem 'mapreduce'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install mapreduce

## Usage

Imagine, you have got two servers with logs like `user_id,event\n`. You need to count events for each user.

So what you could do: run two masters (you can ran 1, 2, 3 etc.), one per server.

Also you should run two workers: one per log.

Ok, here is your master code:

```ruby
# master.rb
require 'map_reduce'
master = Master.new socket: "tcp://server{1,2}.ip:15000"
master.run
```

Run it on each server

```bash
$ ruby master.rb
```

Now worker part. Worker wil emit (map) all data to masters and then reduce it.

```ruby
# worker.rb
require 'map_reduce'
worker = Worker.new masters: ["tcp://server1.ip:15000", "tcp://server2.ip:15000"], type: :sync

File.open("/path/to/log").each do |line|
  user_id, event = line.chomp.split(",")
  # emit some data
  worker.map(user_id, event)
end

# tell masters that you've finished
worker.map_finished

# start some reduce job
worker.reduce do |key, values|
  grouped = values.group_by{ |k| k }
  puts "User: #{key}"
  puts "Events:"
  grouped.each do |event, events|
    puts "#{event}: #{events.size}"
  end
end
```

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request