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
case class IntializeGossipNode(var _nodes: ListBuffer[ActorRef], 
    var _neighborList: List[Int], var _numNodes: Int, 
    var _rumorLimit: Int, var _checker: ActorRef, 
    var _topology: String, var _system: ActorSystem)
case class IntializePushSumNode(var _nodes: ListBuffer[ActorRef], 
    var _neighborList: List[Int], var _numNodes: Int, 
    var _s: Int, var _w: Int, var _changeLimit: Int,
    var _checker: ActorRef, var _topology: String, var _system: ActorSystem)
case class ReceiveGossip() extends GossipMessage
case class ReceivePushSum(_s: Double, _w: Double) extends GossipMessage
case class SendGossip() extends GossipMessage
case class SendPushSum(_s: Double, _w: Double) extends GossipMessage
case class UpdateNeighborList(var nodeName: String) extends GossipMessage

//messages for the checker
case class IntializeChecker(var _nodes: ListBuffer[ActorRef], 
    var _numNodes: Int, var _system: ActorSystem, var _startTime: Long)
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
    var rumorLimit: Int = 10		//for gossip
    var changeLimit: Int = 3		//for push-sum

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
        if("gossip" == algorithm)
          nodes(i) ! IntializeGossipNode(nodes, neighborList, numNodes, rumorLimit, checker, topology, system)
        if("push-sum" == algorithm)
          nodes(i) ! IntializePushSumNode(nodes, neighborList, numNodes, i, 1, changeLimit, checker, topology, system)
      }
    } else if ("2D" == topology) {
      var gridSize: Int = sqrt(numNodes.toDouble).ceil.toInt
      for(i <- 0 to gridSize-1){
        for(j <- 0 to gridSize-1){
          var neighborList: List[Int] = Nil
          neighborList = genNeighborListfor2D(i, j, gridSize)
          if("gossip" == algorithm)
            nodes(i*gridSize+j) ! IntializeGossipNode(nodes, neighborList, numNodes, rumorLimit, checker, topology, system)
          if("push-sum" == algorithm)
            nodes(i*gridSize+j) ! IntializePushSumNode(nodes, neighborList, numNodes, i*gridSize+j, 1, changeLimit, checker, topology, system)
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
          if("gossip" == algorithm)
            nodes(i) ! IntializeGossipNode(nodes, neighborList, numNodes, rumorLimit, checker, topology, system)
          if("push-sum" == algorithm)
            nodes(i) ! IntializePushSumNode(nodes, neighborList, numNodes, i, 1, changeLimit, checker, topology, system)
      }
      
    } else if ("imp2D" == topology) {
      var gridSize: Int = sqrt(numNodes.toDouble).ceil.toInt
      for(i <-0 to gridSize-1){
        for(j <- 0 to gridSize-1){
          var neighborList: List[Int] = Nil
          neighborList = genNeighborListfor2D(i, j, gridSize)
          var theLastNeighbor: Int = genRandExceptCur(0, numNodes-1, i*gridSize+j)
          while(neighborList.contains(theLastNeighbor)){
            theLastNeighbor = genRandExceptCur(0, numNodes-1, i*gridSize+j)
          }
          neighborList = neighborList ::: List(theLastNeighbor)
          if("gossip" == algorithm)
            nodes(i*gridSize+j) ! IntializeGossipNode(nodes, neighborList, numNodes, rumorLimit, checker, topology, system)
          if("push-sum" == algorithm)
            nodes(i*gridSize+j) ! IntializePushSumNode(nodes, neighborList, numNodes, i*gridSize+j, 1, changeLimit, checker, topology, system)
        }
      }
      
    } else {
      println("The topology you input is wrong, please select among full, 2D, line, imp2D.")
      System.exit(1)
    }
    val startTime = System.currentTimeMillis()
    checker ! IntializeChecker(nodes, numNodes, system, startTime)
    
    //select a random node and start spreading messages
    if("gossip" == algorithm){
      nodes(Random.nextInt(nodes.size)) ! ReceiveGossip()
    }else if("push-sum" == algorithm){
      nodes(Random.nextInt(nodes.size)) ! ReceivePushSum(0, 0)
    }else{
      println("The algorithm you input is wrong, please select: gossip or push-sum.")
      System.exit(1)
    }
  }
  
  //generate a random number between first to current-1 and current+1 to last
  def genRandExceptCur(first: Int, last: Int, current: Int): Int = {
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

    var topology: String = null
   
    var nodes = new ListBuffer[ActorRef]()
    var neighborList: List[Int] = Nil
    var numNodes = 0
    var checker: ActorRef = null
    var system: ActorSystem = null
    var receivedMessages: Int = 0
    
    //variables for gossip algorithm
    var rumorLimit: Int = 0
    
    //variables for push-sum algorithm
    var s: Double = 0
    var w: Double = 0
    var changeCounter: Int = 0
    var pushSumThreshold: Double = Math.pow(10, -10)
    var sumEstimate: Double = 0
    var changeLimit: Int = 0
    
    def receive = {
      case IntializeGossipNode(_nodes, _neighborList,  _numNodes, _rumorLimit, _checker, _topology, _system) => {
            nodes = _nodes
            neighborList = _neighborList
            numNodes = _numNodes
            rumorLimit = _rumorLimit
            checker = _checker
            topology = _topology
            system = _system
          }
      
      case IntializePushSumNode(_nodes, _neighborList, _numNodes, _s, _w, _changeLimit, _checker, _topology, _system) => {
        nodes = _nodes
        neighborList = _neighborList
        numNodes = _numNodes
        s = _s
        w = _w
        changeLimit = _changeLimit
        sumEstimate = s / w
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
          //writer.println(self.path.name + " to "+nodes(randNeighbor).path.name+" messages account: "+receivedMessages)
          nodes(randNeighbor) ! ReceiveGossip()
        }
      }
      
      case ReceivePushSum(_s: Double, _w: Double) => {
        if(sender() != self &&  receivedMessages == 0){
          receivedMessages += 1
          s += _s
          w += _w
          sumEstimate = s/w
          context.system.scheduler.schedule(0 milliseconds, 1 milliseconds, self, SendPushSum(s, w))
        //} else if(sender() != self && changeCounter < changeLimit && neighborList.size > 0){
        } else if(sender() != self && neighborList.size > 0){
          receivedMessages += 1
          var oldSumEstimate: Double = s/w
          
          s += _s
          w += _w
          sumEstimate = s/w
          
          writer.println("sum estimate for "+self.path.name+ " is: "+sumEstimate)
          
          if(Math.abs(sumEstimate - oldSumEstimate) < pushSumThreshold)
            changeCounter +=1
            else
              changeCounter = 0
          
          if(changeCounter >= changeLimit){
            writer.println("sum estimate for "+self.path.name+ " while dying is: "+sumEstimate)
            //println("sum estimate for "+self.path.name+ " while dying is: "+sumEstimate)
            //deactive this actor
            checker ! CheckActiveActor()
            for(i <- 0 to neighborList.size-1)
	            nodes(neighborList(i)) ! UpdateNeighborList(self.path.name)
          }
          
        }
      }
      
      case SendPushSum(_s: Double, _w: Double) => {
//        if(sender() == self && changeCounter < changeLimit && neighborList.size > 0){
        if(sender() == self && neighborList.size > 0){
          s = s/2
          w = w/2
          var randNeighbor = neighborList(Random.nextInt(neighborList.size))
          nodes(randNeighbor) ! ReceivePushSum(s, w)
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
    
    var startTime : Long = 0
    var endTime : Long = 0
    
    var nodes = new ListBuffer[ActorRef]()
    var numNodes = 0
    var system: ActorSystem = null
    def receive = {
      case IntializeChecker(_nodes, _numNodes, _system, _startTime) => {
        nodes = _nodes
        numNodes = _numNodes
        system = _system
        startTime = _startTime
        for(i <-0 to numNodes-1)
          activeNodeList = activeNodeList ::: List(nodes(i).path.name)
      }
      
      case ActorStartSendingMessage() => {
        numActorStartSendingMessage += 1
        if(numActorStartSendingMessage == numNodes){
          writer.println("convergence. Situation 1.")
          endTime = System.currentTimeMillis()
          var elaspedTime = endTime-startTime
          println("elasped_time: "+elaspedTime)
          writer.println("elapsed_time: "+elaspedTime)
          system.shutdown()
          writer.close()
        }
      }
      
      case CheckActiveActor() => {
        writer.println("checkActiveActor" + sender().path.name)
        activeNodeList = activeNodeList.filter(x => x != sender().path.name)
        //println("still active nodes:"+activeNodeList.size)
        writer.println("still active nodes:"+activeNodeList.size+" "+sender())
        if (0 == activeNodeList.size) {
          writer.println("convergence. Situation 2.")
          endTime = System.currentTimeMillis()
          var elaspedTime = endTime-startTime
          println("elasped_time: "+elaspedTime+" "+sender()+" activeNode size: "+activeNodeList.size)
          writer.println("elasped_time: "+elaspedTime+" "+sender()+" activeNode size: "+activeNodeList.size)
          system.shutdown()
          writer.close()
        }
      }
      
    }
  }
}