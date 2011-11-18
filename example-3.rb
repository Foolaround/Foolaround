$: << 'vendor/akka'

require 'rubygems'
require 'java'
require 'akka-modules-1.0'
#require 'akka'
require 'net/http'
require 'awesome_print'
require 'neo4j'
require 'json'

module Akka
  include_package 'akka.actor'
  include_package 'akka.amqp'
  include_package 'akka.dispatch'
end

$: << File.join(File.dirname(__FILE__), 'vendor/akka/lib_managed/compile')
java_import 'akka.actor.Actors'
java_import 'akka.actor.ActorRef'
java_import 'akka.actor.UntypedActor'
java_import 'akka.actor.UntypedActorFactory'
java_import 'akka.routing.CyclicIterator'
java_import 'akka.routing.InfiniteIterator'
java_import 'akka.routing.Routing'
java_import 'akka.routing.UntypedLoadBalancer'
java_import java.util.concurrent.CountDownLatch

class Url < Neo4j::Rails::Model
  attr_accessor :url
  url_regex = /^http:\/\//
  has_n(:links).to(Url)
  property :url
  index :url

  validates :url, :format => {:with => url_regex}
end

def actorOf(&code)
  Actors.actorOf(Class.new do
                     include UntypedActorFactory
                     define_method(:create) do |*args|
                       code[*args]
                     end
                   end.new)
end

class ActorN < Akka::UntypedActor
  def self.create(*args)
      new *args
    end

  def onReceive(msg)
    data = String.from_java_bytes(msg.payload)
    puts "Saving " + data
#    Neo4j::Transaction.run do
#      node = Neo4j::Node.new(:url => data)
#      puts node.inspect
#    end
  end
end

class ActorR < Akka::UntypedActor
  def self.spawn
    Akka::Actors.actorOf { new }.tap do |actor|
      actor.setDispatcher(Akka::Dispatchers.newThreadBasedDispatcher(actor))
      actor.start
    end
  end

  def onReceive(msg)
    data = String.from_java_bytes(msg.payload)
    puts "Received " + data
    Neo4j::Transaction.run do
      google = Url.find_by_url("http://google.de") or raise "google not found"
      new_url = Url.new(:url => data)
      new_url.save
      google.links.build :url=>data
   #   raise " ====> FOUND: " + google.inspect
    #  node.outgoing(:links_to)  << Neo4j::Node.find(:url => to) unless to.nil?
      newgoogle = Url.find_by_url("http://google.de")
      newgoogle.links.each {|n| puts "  => " + n.url}
    end
    puts "  == found google node " + google.url + " =="
    puts "  == created node ==" + node.url
    sleep 2
  end
end

Neo4j::Transaction.run do
  $google = Url.create! :url => "http://google.de"
end

sleep 2
puts ">> Starting <<"

connection = Akka::AMQP.newConnection(Akka::AMQP::ConnectionParameters.new)
1.times do
  params = Akka::AMQP::ConsumerParameters.new(
    "create", ActorR.spawn, "create", Akka::AMQP::ActiveDeclaration.new(false, false), true, Akka::AMQP::ChannelParameters.new
  )
  consumer = Akka::AMQP.newConsumer(connection, params)
end

saver = Actors.actorOf(ActorN).start