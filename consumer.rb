#!/usr/bin/env ruby
require 'rubygems'
require 'kafka'
require 'optparse'

options = { timeout: 30 }

OptionParser.new do |opts|
  opts.on('-g GROUP', '--group GROUP') do |group|
    options[:group_id] = group
  end
end.parse!

client = Kafka.new('localhost:9092', client_id: 'my-applycation')
consumer = client.consumer(group_id: options[:group_id] || 'some-group')

trap("TERM") { consumer.stop }

consumer.subscribe('TutorialTopic', start_from_beginning: false)
consumer.each_message do |m|
  consumer.mark_message_as_processed(m)
  puts [m.partition, m.offset, m.key, m.value, m.headers, m.create_time,].join(' - ')
end
