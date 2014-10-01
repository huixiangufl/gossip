package gossip

import akka.actor._
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
import akka.actor.Props
import scala.collection.mutable.ListBuffer

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

    val numNodes: Int = args(0).toInt
    val topology: String = args(1).toString()
    val algorithm: String = args(2).toString()
    
    var rumorLimit: Int = 10


    val system = ActorSystem("GossipCommunicationSystem")

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
      checker ! IntializeChecker(nodes, numNodes, rumorLimit, system)

    } else if ("2D" == topology) {

    } else if ("line" == topology) {
      
    } else if ("imp2D" == topology) {

    } else {
      println("The topology you input is wrong, please select among full, 2D, line, imp2D.")
      System.exit(1)
    }

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
        if(receivedMessages < rumorLimit){
          nodes(Random.nextInt(nodes.size)) ! Gossip
        }
        else {
          checker ! CheckGossipNumber()
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
        if(numActiveActors == 0){
          system.shutdown()
        }
    
     
        
    }
  }

}