package gossip

import scala.concurrent.duration._
import java.lang.NumberFormatException
import akka.routing.RoundRobinRouter
import java.security.MessageDigest
import scala.util.control._
import java.io._
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
//messages for the node
case class IntializeNode(var _nodes: ListBuffer[ActorRef], 
    var _neighborList: List[Int], var _numNodes: Int, 
    var _rumorLimit: Int, var _checker: ActorRef, 
    var _topology: String, var _system: ActorSystem)
case class ReceiveGossip() extends GossipMessage
case class SendGossip() extends GossipMessage
case class UpdateNeighborList(var nodeName: String) extends GossipMessage

//messages for the checker
case class IntializeChecker(var _nodes: ListBuffer[ActorRef], 
    var _numNodes: Int, var _rumorLimit: Int, var _system: ActorSystem)
case class ActorStartSendingMessage() extends GossipMessage
case class CheckActiveActor() extends GossipMessage


object project2 {

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Invalid number of arguments. Run with numNodes, topology and algorithm")
      System.exit(1)
    }
    
    val writer = new PrintWriter("test.txt")

    var numNodes: Int = args(0).toInt
    var topology: String = args(1).toString()
    var algorithm: String = args(2).toString()
    var rumorLimit: Int = 10

    val system = ActorSystem("GossipCommunicationSystem")
    
    if("2D" == topology || "imp2D" == topology){
      var gridSize: Int = sqrt(numNodes.toDouble).ceil.toInt
      numNodes = gridSize * gridSize
    }

    val nodes = new ListBuffer[ActorRef]()
    for(i<-0 to numNodes-1){
      nodes += system.actorOf(Props(new Node(writer)), name=i.toString)
    }
        
    val checker = system.actorOf(Props(new Checker(writer)), name="Checker")

    if ("full" == topology) {
      for (i <- 0 to numNodes-1) {
        var neighborList: List[Int] = Nil
        for (j <- 0 to numNodes-1) {
          if (j != i)
            neighborList = neighborList ::: List(j)
        }
        nodes(i) ! IntializeNode(nodes, neighborList, numNodes, rumorLimit, checker, topology, system)
      }

    } else if ("2D" == topology) {
      var gridSize: Int = sqrt(numNodes.toDouble).ceil.toInt
      for(i <- 0 to gridSize-1){
        for(j <- 0 to gridSize-1){
          var neighborList: List[Int] = Nil
          neighborList = genNeighborListfor2D(i, j, gridSize)
          nodes(i*gridSize+j) ! IntializeNode(nodes, neighborList, numNodes, rumorLimit, checker, topology, system)
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
        nodes(i) ! IntializeNode(nodes, neighborList, numNodes, rumorLimit, checker, topology, system)
      }
      
    } else if ("imp2D" == topology) {
      //implement here
      var gridSize: Int = sqrt(numNodes.toDouble).ceil.toInt
      for(i <-0 to gridSize-1){
        for(j <- 0 to gridSize-1){
          var neighborList: List[Int] = Nil
          neighborList = genNeighborListfor2D(i, j, gridSize)
          var theLastNeighbor: Int = genRandExceptCurrent(0, numNodes-1, i*gridSize+j)
          neighborList = neighborList ::: List(theLastNeighbor)
          println(neighborList)
        }
      }

    } else {
      println("The topology you input is wrong, please select among full, 2D, line, imp2D.")
      System.exit(1)
    }
    checker ! IntializeChecker(nodes, numNodes, rumorLimit, system)
    
    if("gossip" == algorithm){
      nodes(0) ! ReceiveGossip()
    }else if("push-sum" == algorithm){
      //implement push-sum
    }else{
      println("The algorithm you input is wrong, please select: gossip or push-sum.")
      System.exit(1)
    }
  }
  
  //generate a random number between first to current-1 and current+1 to last
  def genRandExceptCurrent(first: Int, last: Int, current: Int): Int = {
    val range1 = first to current-1
    val range2 = current+1 to last
    val range: List[Int] = range1.toList ::: range2.toList
    range(Random.nextInt(range.size))
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
  

  class Node (var writer: PrintWriter) extends Actor {
    var receivedMessages: Int = 0
    var topology: String = null
   
    var nodes = new ListBuffer[ActorRef]()
    var neighborList: List[Int] = Nil
    var numNodes = 0
    var rumorLimit = 0
    var checker: ActorRef = null
    var system: ActorSystem = null
    
    def receive = {
      case IntializeNode(_nodes, _neighborList,  _numNodes, _rumorLimit, _checker, _topology, _system) => {
            nodes = _nodes
            neighborList = _neighborList
            numNodes = _numNodes
            rumorLimit = _rumorLimit
            checker = _checker
            topology = _topology
            system = _system
          }

      case ReceiveGossip() => {
        if (sender() != self && receivedMessages == 0 && neighborList.size > 0) {
          checker ! ActorStartSendingMessage()
          receivedMessages += 1
          context.system.scheduler.schedule(0 milliseconds, 1 milliseconds, self, SendGossip())
        } else if (sender() != self && receivedMessages < rumorLimit && neighborList.size > 0) {
          receivedMessages += 1
          if (receivedMessages == rumorLimit) {
//            //to solve problem in line topo: 0-1-2-3-4-5, when 0 and 1 receives enough messages and dead
//            //the rest of the network cannot receive gossip any more, and stuck
//            if ("line" == topology) {
//              for (i <- 0 to neighborList.size - 1) {
//                if (0 == nodes(neighborList(i)).receivedMessages)
//                  nodes(neighborList(i)) ! ReceiveGossip()
//              }
//            }
            //send the checker to check the number of active actors
            checker ! CheckActiveActor()
            //update the neighbor list of all current actor's neighbors
            for (i <- 0 to neighborList.size - 1)
              nodes(neighborList(i)) ! UpdateNeighborList(self.path.name)
            //from now on, this actor is dead: 
            //we don't kill it, but it can't send and receive messages because of (receivedMessages < rumorLimit) condition 
          }
        }
      }
      
      case SendGossip() => {
        if(sender() == self && receivedMessages < rumorLimit && neighborList.size > 0){
          //send message to a random neighbor
          var randNeighbor = neighborList(Random.nextInt(neighborList.size))
          writer.println(self.path.name + " to "+nodes(randNeighbor).path.name+" messages account: "+receivedMessages)
          nodes(randNeighbor) ! ReceiveGossip()
        }
      }
      
      case UpdateNeighborList(nodeName) => {
        neighborList = neighborList.filter(x => x != nodeName.toInt)
        if(0 == neighborList.size){
          checker ! CheckActiveActor()
          //neighorList.size==0 also means this node is dead
          //we don't kill it, but it cann't send and receive messages because of (neighborList.size > 0) condition
        }
      }
      
    }
  }

  class Checker (var writer: PrintWriter) extends Actor {
    var numActorStartSendingMessage = 0
    var activeNodeList: List[String] = Nil
    
    var nodes = new ListBuffer[ActorRef]()
    var numNodes = 0
    var rumorLimit = 0
    var system: ActorSystem = null
    def receive = {
      case IntializeChecker(_nodes, _numNodes, _rumorLimit, _system) => {
        nodes = _nodes
        numNodes = _numNodes
        rumorLimit = _rumorLimit
        system = _system
        for(i <-0 to numNodes-1)
          activeNodeList = activeNodeList ::: List(nodes(i).path.name)
      }
      
      case ActorStartSendingMessage() => {
        numActorStartSendingMessage += 1
        if(numActorStartSendingMessage == numNodes){
          writer.println("convergence. Situation 1.")
          system.shutdown()
          writer.close()
        }
      }
      
      case CheckActiveActor() => {
        writer.println("checkActiveActor" + sender().path.name)
        activeNodeList = activeNodeList.filter(x => x != sender().path.name)
        if (0 == activeNodeList.size) {
          writer.println("convergence. Situation 2.")
          system.shutdown()
          writer.close()
        }
      }
      
    }
  }

}