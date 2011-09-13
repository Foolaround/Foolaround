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
    # Do something
    # send to save - Actor
    puts "Sending " + data
    saver.sendOneWay data
  end
end

connection = Akka::AMQP.newConnection(Akka::AMQP::ConnectionParameters.new)


10.times do
  params = Akka::AMQP::ConsumerParameters.new(
    "create", ActorR.spawn, "create", Akka::AMQP::ActiveDeclaration.new(false, false), true, Akka::AMQP::ChannelParameters.new
  )
  consumer = Akka::AMQP.newConsumer(connection, params)
end

saver = Actors.actorOf(ActorN).start