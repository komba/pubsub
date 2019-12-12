#!/usr/bin/env ruby

require 'rubygems'
require 'kafka'
require 'optparse'

kafka = Kafka.new('localhost:9092', client_id: 'my-application')
options = {}

OptionParser.new do |opts|
  opts.on('-t TIMEOUT', '--timeout TIMEOUT') do |timeout|
    options[:timeout] = timeout.to_i
  end
end.parse!

producer = kafka.async_producer(delivery_threshold: 10, required_acks: 1, ack_timeout: options[:timeout] || 5)

12.times do |i|
  producer.produce("hello #{i}", topic: 'TutorialTopic', key: :yo, headers: {'ha' => "ja #{Time.now.to_s}"}, partition: i % 4)
end

producer.deliver_messages
producer.shutdown
