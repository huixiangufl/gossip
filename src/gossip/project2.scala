package gossip

import scala.concurrent.duration._
import java.lang.NumberFormatException
import akka.routing.RoundRobinRouter
import java.security.MessageDigest
import scala.util.control._
import java.net._
import scala.math._
import scala.util.Random
import akka.actor.Actor
import akka.actor.Props
import akka.actor._
import akka.actor.ActorRef
import akka.actor.ActorSystem
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global


sealed trait GossipMessage
case class IntializeNode(var _nodes: ListBuffer[ActorRef], 
    var _neighborList: List[Int], var _numNodes: Int, 
    var _rumorLimit: Int, var _checker: ActorRef, var _system: ActorSystem)
case class Gossip() extends GossipMessage
case class IntializeChecker(var _nodes: ListBuffer[ActorRef], var _numNodes: Int, var _rumorLimit: Int, var _system: ActorSystem)
case class CheckGossipNumber() extends GossipMessage
case class StopSystem() extends GossipMessage // not sure whether should use it

object project2 {

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Invalid number of arguments. Run with numNodes, topology and algorithm")
      System.exit(1)
    }

    var numNodes: Int = args(0).toInt
    var topology: String = args(1).toString()
    var algorithm: String = args(2).toString()
    var rumorLimit: Int = 10

    val system = ActorSystem("GossipCommunicationSystem")
    
    if("2D" == topology){
      var gridSize: Int = sqrt(numNodes.toDouble).ceil.toInt
      numNodes = gridSize * gridSize
    }

    val nodes = new ListBuffer[ActorRef]()
    for(i<-0 to numNodes-1){
      nodes += system.actorOf(Props[Node])
    }
        
    val checker = system.actorOf(Props[Checker])

    if ("full" == topology) {
      for (i <- 0 to numNodes-1) {
        var neighborList: List[Int] = Nil
        for (j <- 0 to numNodes-1) {
          if (j != i)
            neighborList = neighborList ::: List(j)
        }
        nodes(i) ! IntializeNode(nodes, neighborList, numNodes, rumorLimit, checker, system)
      }

    } else if ("2D" == topology) {
      var gridSize: Int = sqrt(numNodes.toDouble).ceil.toInt
      for(i <- 0 to gridSize-1){
        for(j <- 0 to gridSize-1){
          var neighborList: List[Int] = Nil
          neighborList = genNeighborListfor2D(i, j, gridSize)
          nodes(i*gridSize+j) ! IntializeNode(nodes, neighborList, numNodes, rumorLimit, checker, system)
        }
      }
      
    } else if ("line" == topology) {  
      //asssume the numNodes cannot be 1
      for(i <- 0 to numNodes-1) {
        var neighborList: List[Int] = Nil
        if(0 == i)
          neighborList = neighborList ::: List(i+1)
        else if(numNodes-1 == i)
          neighborList = neighborList ::: List(i-1)
        else
          neighborList = neighborList ::: List(i-1) ::: List(i+1)
        nodes(i) ! IntializeNode(nodes, neighborList, numNodes, rumorLimit, checker, system)
      }
      
    } else if ("imp2D" == topology) {

    } else {
      println("The topology you input is wrong, please select among full, 2D, line, imp2D.")
      System.exit(1)
    }
    checker ! IntializeChecker(nodes, numNodes, rumorLimit, system)
    
    if("gossip" == algorithm){
      nodes(0) ! Gossip()
    }else if("push-sum" == algorithm){
      
    }else{
      println("The algorithm you input is wrong, please select: gossip or push-sum.")
      System.exit(1)
    }
  }

  def genNeighborListfor2D(i: Int, j: Int, gridSize: Int): List[Int] = {
    var neighborList: List[Int] = Nil
    if (0 == i) {
      if (0 == j)
        neighborList = neighborList ::: List(j + 1) ::: List(j + gridSize)
      else if (gridSize - 1 == j)
        neighborList = neighborList ::: List(j - 1) ::: List(j + gridSize)
      else
        neighborList = neighborList ::: List(j - 1) ::: List(j + 1) ::: List(j + gridSize)
    } else if (gridSize - 1 == i) {
      if (0 == j)
        neighborList = neighborList ::: List(i * gridSize + j + 1) ::: List(i * gridSize + j - gridSize)
      else if (gridSize - 1 == j)
        neighborList = neighborList ::: List(i * gridSize + j - 1) ::: List(i * gridSize + j - gridSize)
      else
        neighborList = neighborList ::: List(i * gridSize + j - 1) ::: List(i * gridSize + j + 1) ::: List(i * gridSize + j - gridSize)
    } else {
      if (0 == j)
        neighborList = neighborList ::: List(i * gridSize + j + 1) ::: List(i * gridSize + j - gridSize) ::: List(i * gridSize + j + gridSize)
      else if (gridSize - 1 == j)
        neighborList = neighborList ::: List(i * gridSize + j - 1) ::: List(i * gridSize + j - gridSize) ::: List(i * gridSize + j + gridSize)
      else
        neighborList = neighborList ::: List(i * gridSize + j - 1) ::: List(i * gridSize + j + 1) ::: List(i * gridSize + j - gridSize) ::: List(i * gridSize + j + gridSize)
    }
    neighborList
  }
  

  class Node extends Actor {
    
    var receivedMessages: Int = 0
   
    var nodes = new ListBuffer[ActorRef]()
    var neighborList: List[Int] = Nil
    var numNodes = 0
    var rumorLimit = 0
    var checker: ActorRef = null
    var system: ActorSystem = null
    
    def receive = {
      case IntializeNode(_nodes, _neighborList,  _numNodes, _rumorLimit, _checker, _system) => {
            nodes = _nodes
            neighborList = _neighborList
            numNodes = _numNodes
            rumorLimit = _rumorLimit
            checker = _checker
            system = _system
          }
      case Gossip() => {
        if(sender() != self && receivedMessages == 0){//create a alarm scheduler to send message every interval
          println("num: 0"+receivedMessages)
          receivedMessages += 1
          context.system.scheduler.schedule(0 milliseconds, 1000 milliseconds, self, Gossip())
        }else 
          if(sender() != self && receivedMessages < rumorLimit){//current node receives a message from another node
            println("num: i"+receivedMessages)
            receivedMessages += 1
            if(receivedMessages == rumorLimit)
              checker ! CheckGossipNumber()
          }
          else
            if(sender() == self && receivedMessages < rumorLimit){//sends a message to another random neighbor
              nodes(Random.nextInt(nodes.size)) ! Gossip()
            }
      }
         
    }
  }

  class Checker extends Actor {
    var numActiveActors = 0
    
    var nodes = new ListBuffer[ActorRef]()
    var numNodes = 0
    var rumorLimit = 0
    var system: ActorSystem = null
    def receive = {
      case IntializeChecker(_nodes, _numNodes, _rumorLimit, _system) => {
        numActiveActors = _numNodes
        nodes = _nodes
        numNodes = _numNodes
        rumorLimit = _rumorLimit
        system = _system
      }
      
      case CheckGossipNumber() =>
        numActiveActors -= 1
        if(1 == numActiveActors){
          println("convergence.")
          system.shutdown()
        }
    }
  }

}