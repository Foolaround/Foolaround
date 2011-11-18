# Small script to enqueue messages for example-3.rb

require 'rubygems'
require 'bunny'
require 'json'

bunch_of_urls = [
  "http://www.webpop.com", "http://www.engineyard.com/", "http://www.jruby.org/", "http://akka.io/",
  "http://www.madinspain.com", "http://www.domestika.org", "http://www.google.com"
]

bunny = Bunny.new
bunny.start
queue = bunny.queue('create')
exchange = bunny.exchange ""

bunch_of_urls.each do |url|
  exchange.publish url, :key => 'create'
end