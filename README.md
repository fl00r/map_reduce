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

## Introduction

MapReduce has got three entities:

* Master
* Mapper
* Reducer

Perhaps later Manager will be presented to synchronyse Reducers.

### Master

Master is a process who accepts emmited by Mappers data, sorts it and sends grouped data to Reducers. One Master can serve multiple tasks (multiple Mappers clusters).

To run Master you could specify following options

* TCP/IPC/Unix (TCP if you need to work over Network) socket address to bind; Workers will connect to this address (default is `tcp://127.0.0.1:5555`)
* Logs folder to store temprorary logs with received data (default is `/tmp/map_reduce`); be sure to add read/write access for proccess to this folder
* Delimeter for key, value (default is `\t`); sometimes you want to set your own delimeter if TAB could be found in your key

Also you could define some blocks of code. It could be useful for getting some stats from Master.

* after_map - this block will be executed after Master received emmited data
* after_reduce - this block will be executed after Master sended data to Reducer

All blocks recieves `|key, value, task_name|`

Simple Master

```ruby
require 'map_reduce'
# Default params
master = MapReduce::Master.new
# Same as
master = MapReduce::Master.new socket: "tcp://127.0.0.1:555",
                               log_folder: "/tmp/map_reduce",
                               delimiter: "\t"

# Define some logging after map and reduce
master.after_map do |key, value, task|
  puts "Task: #{task}, received key: #{key}"
end

master.after_reduce do |key, values, task|
  puts "Task: #{task}, for key: #{key} was sended #{values.size} items"
end

# Run Master
master.run
```

### Mapper

Mapper emmits data to masters. It could read log, database, or answer to phone calls. What should Mapper know is how to connect to Masters and it is ready to go. Also you could choose mode in which you want to work. Worker works asynchronously, but you could choose if you want to write callbacks (pure EventMachine) or you prefer to wrap it in Fibers (em-synchrony, for example). Also you could specify task name to worker if Masters serve many tasks.

* masters - is an array of all available Masters' sockets
* type - `:em` or `:sync` (`:em` id default)
* task - task name, default is `nil`

For example, we have got some Web application (shop) and you want to explore which goods people look with each other.

(Let's suppose that web server is running under EventMachine and each request is spawned in Fiber)

```ruby
# Define somewhere your worker
require 'map_reduce'

@worker = MapReduce::Worker.new type: :sync, 
            masters: ["tcp://192.168.1.1:5555", "tcp://192.168.1.2:5555"],
            task: "goods"

# And use it in your Web App
get "/good/:id" do
  @good = Good.find(params[:id])
  # Send current user's id and good's id
  @worker.map(current_user.id, @good.id)
  haml :good
end
```

Also Mapper has got `wait_for_all` method. If you are mapping data not permanently and you need to know when all mappers finished mapping data you should call this method.

```ruby
rand(1000000).times do |i|
  mapper.map(i, 1)
end
mapper.wait_for_all
```

So you will be blocked till all servers will finish mapping data. Then you could start reducing data, for example.

### Reducer

Reducer is a guy who receives grouped data from Masters. In our previous example with shop Reducer will recieve all goods that current user visited for every user. So now you can use some ML algorithms, or append data to existing GoodsGraph or whatever science.

As Worker Reducer should know masters sockets addresses, type of connection and task name if needed (if Mapper emits data with named task, Reducer should specify it as well).

```ruby
require 'em-synchrony'
require 'map_reduce'
# initialize one
reducer = MapReduce::Reducer.new type: :sync, 
            masters: ["tcp://192.168.1.1:5555", "tcp://192.168.1.2:5555"],
            task: "goods"

# Lets give masters to collect some data between each reduce
EM.synchrony do
  while true
    reducer.reduce do |key, values|
      # You can do some magick here
      puts "User: #{key}, visited #{values} today"
    end
    EM::Synchrony.sleep(60 * 60 * 3)
  end
end
```

## Usage

So. Generally you need to specify two thigs:

* What to map
* How to reduce

And implement it with given primitives. 

Maybe the simplest example should be count of page visits (video views, tracks listens) for each article. In the case you have got millions of visits incrementing your data for each visit in RDBMS could be very expensive operation. So updating one/two times per day in some cases is a good choice. So we have got bunch of logs `article_id, user_id, timestamp` on each frontend and we need to count visits for each article and increment it in database.


So on each server you could run Master, Mapper and Reducer.

You could even combine Mapper and Reducer in one process, becuse you need to fire Reducer right after you have finished your map phase.

```ruby
# master.rb
require 'map_reduce'

MapReduce::Master.new(socket: "#{current_ip}:5555")
```

```ruby
# map_reducer.rb
require 'map_reduce'
require 'em-synchrony'

@mapper = MapReduce::Mapper.new masters: [ ... ], type: :sync
@reducer = MapReduce::Reducer.new masters: [ ... ], type: :sync

EM.synchrony do
  # Run process each 12 hours
  EM::Synchrony.add_periodic_timer(60*60*12) do
    File.open("/path/to/log").each do |line|
      article_id, user_id, timestamp = line.chomp.split(", ")
      @mapper.map(article_id, 1)
    end

    @mapper.wait_for_all

    @reducer.reduce do |key, values|
      # How many time article was visited
      count = values.size
      # Let's increment this value
      Article.increment(visits: count)
    end
  end
end
```

And run them

    $ ruby master.rb
    $ ruby map_reducer.rb


## Summary

It is pretty simple implementation of map reduce and it doesn't solve synchronizing, loosing connectivity, master/worker/reducer failing problems. They are totally up to developers. And there is Hadoop for really big map reduce problems.

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request