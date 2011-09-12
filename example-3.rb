$: << 'vendor/akka'

require 'rubygems'
require 'java'
require 'akka-modules-1.0'
require 'net/http'
require 'awesome_print'
require 'neo4j'
require 'json'

module Akka
  include_package 'akka.actor'
  include_package 'akka.amqp'
  include_package 'akka.dispatch'
end


class Actor < Akka::UntypedActor
  def self.spawn
    Akka::Actors.actorOf { new }.tap do |actor|
      actor.setDispatcher(Akka::Dispatchers.newThreadBasedDispatcher(actor))
      actor.start
    end
  end
  
  def onReceive(msg)
    data = String.from_java_bytes(msg.payload)
    Neo4j::Transaction.run do
      node = Neo4j::Node.new(:url => data)
      puts node.inspect
    end
  end
end

connection = Akka::AMQP.newConnection(Akka::AMQP::ConnectionParameters.new)


1.times do
  params = Akka::AMQP::ConsumerParameters.new(
    "fetch.url", Actor.spawn, "fetch.url", Akka::AMQP::ActiveDeclaration.new(false, false), true, Akka::AMQP::ChannelParameters.new
  )
  consumer = Akka::AMQP.newConsumer(connection, params)
end