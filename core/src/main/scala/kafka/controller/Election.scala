/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.controller

import kafka.api.LeaderAndIsr
import org.apache.kafka.common.TopicPartition

import scala.collection.Seq

case class ElectionResult(topicPartition: TopicPartition, leaderAndIsr: Option[LeaderAndIsr], liveReplicas: Seq[Int])

object Election {

  private def leaderForOffline(partition: TopicPartition,
                               leaderAndIsrOpt: Option[LeaderAndIsr],
                               uncleanLeaderElectionEnabled: Boolean,
                               controllerContext: ControllerContext): ElectionResult = {

    val assignment = controllerContext.partitionReplicaAssignment(partition)
    //处于存活状态的ISR副本
    val liveReplicas = assignment.filter(replica => controllerContext.isReplicaOnline(replica, partition))
    leaderAndIsrOpt match {
      case Some(leaderAndIsr) =>
        val isr = leaderAndIsr.isr
        //从当前分区副本列表中寻找首个处于存活状态的ISR副本
        val leaderOpt = PartitionLeaderElectionAlgorithms.offlinePartitionLeaderElection(
          assignment, isr, liveReplicas.toSet, uncleanLeaderElectionEnabled, controllerContext)
        val newLeaderAndIsrOpt = leaderOpt.map { leader =>
          val newIsr = if (isr.contains(leader)) isr.filter(replica => controllerContext.isReplicaOnline(replica, partition))
          else List(leader)
          leaderAndIsr.newLeaderAndIsr(leader, newIsr)
        }
        //返回选举结果
        ElectionResult(partition, newLeaderAndIsrOpt, liveReplicas)

      case None =>
        ElectionResult(partition, None, liveReplicas)
    }
  }

  /**
   * Elect leaders for new or offline partitions.
   *
   * @param controllerContext Context with the current state of the cluster
   * @param partitionsWithUncleanLeaderElectionState A sequence of tuples representing the partitions
   *                                                 that need election, their leader/ISR state, and whether
   *                                                 or not unclean leader election is enabled
   * 返回 ElectionResult 列表
   * @return The election results
   */
  def leaderForOffline(
    controllerContext: ControllerContext,
    //参数 2 表示一个序列，并且序列中元素的类型为(TopicPartition, Option[LeaderAndIsr], Boolean)
    partitionsWithUncleanLeaderElectionState: Seq[(TopicPartition, Option[LeaderAndIsr], Boolean)]
  ): Seq[ElectionResult] = {

    //通过map()把Seq[](TopicPartition, Option[LeaderAndIsr], Boolean) 转化为 Seq[ElectionResult]
    partitionsWithUncleanLeaderElectionState.map {
      case (partition, leaderAndIsrOpt, uncleanLeaderElectionEnabled) =>
        //调用选举算法，从当前分区副本列表中寻找首个处于存活状态的ISR副本
        leaderForOffline(partition, leaderAndIsrOpt, uncleanLeaderElectionEnabled, controllerContext)
    }
  }

  private def leaderForReassign(partition: TopicPartition,
                                leaderAndIsr: LeaderAndIsr,
                                controllerContext: ControllerContext): ElectionResult = {
    val targetReplicas = controllerContext.partitionFullReplicaAssignment(partition).targetReplicas
    val liveReplicas = targetReplicas.filter(replica => controllerContext.isReplicaOnline(replica, partition))
    val isr = leaderAndIsr.isr
    val leaderOpt = PartitionLeaderElectionAlgorithms.reassignPartitionLeaderElection(targetReplicas, isr, liveReplicas.toSet)
    val newLeaderAndIsrOpt = leaderOpt.map(leader => leaderAndIsr.newLeader(leader))
    ElectionResult(partition, newLeaderAndIsrOpt, targetReplicas)
  }

  /**
   * Elect leaders for partitions that are undergoing reassignment.
   *
   * @param controllerContext Context with the current state of the cluster
   * @param leaderAndIsrs A sequence of tuples representing the partitions that need election
   *                                     and their respective leader/ISR states
   *
   * @return The election results
   */
  def leaderForReassign(controllerContext: ControllerContext,
                        leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)]): Seq[ElectionResult] = {
    leaderAndIsrs.map { case (partition, leaderAndIsr) =>
      leaderForReassign(partition, leaderAndIsr, controllerContext)
    }
  }

  private def leaderForPreferredReplica(partition: TopicPartition,
                                        leaderAndIsr: LeaderAndIsr,
                                        controllerContext: ControllerContext): ElectionResult = {
    val assignment = controllerContext.partitionReplicaAssignment(partition)
    val liveReplicas = assignment.filter(replica => controllerContext.isReplicaOnline(replica, partition))
    val isr = leaderAndIsr.isr
    val leaderOpt = PartitionLeaderElectionAlgorithms.preferredReplicaPartitionLeaderElection(assignment, isr, liveReplicas.toSet)
    val newLeaderAndIsrOpt = leaderOpt.map(leader => leaderAndIsr.newLeader(leader))
    ElectionResult(partition, newLeaderAndIsrOpt, assignment)
  }

  /**
   * Elect preferred leaders.
   *
   * @param controllerContext Context with the current state of the cluster
   * @param leaderAndIsrs A sequence of tuples representing the partitions that need election
   *                                     and their respective leader/ISR states
   *
   * @return The election results
   */
  def leaderForPreferredReplica(controllerContext: ControllerContext,
                                leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)]): Seq[ElectionResult] = {
    leaderAndIsrs.map { case (partition, leaderAndIsr) =>
      leaderForPreferredReplica(partition, leaderAndIsr, controllerContext)
    }
  }

  private def leaderForControlledShutdown(partition: TopicPartition,
                                          leaderAndIsr: LeaderAndIsr,
                                          shuttingDownBrokerIds: Set[Int],
                                          controllerContext: ControllerContext): ElectionResult = {
    //根据partition获取分区副本分配情况
    val assignment = controllerContext.partitionReplicaAssignment(partition)
    //过滤出分区所有存活的副本(正在关闭中的Broker里面的副本也要算进去)
    val liveOrShuttingDownReplicas = assignment.filter(replica =>
      controllerContext.isReplicaOnline(replica, partition, includeShuttingDownBrokers = true))
    val isr = leaderAndIsr.isr
    // 选择isr列表中第一个副本作为leader，排除掉属于shuttingDownBrokers中的broker
    val leaderOpt = PartitionLeaderElectionAlgorithms.controlledShutdownPartitionLeaderElection(assignment, isr,
      liveOrShuttingDownReplicas.toSet, shuttingDownBrokerIds)
    //构造新的ISR列表，在之前的isr列表中将 正在被关闭的Broker 里面的副本给剔除掉
    val newIsr = isr.filter(replica => !shuttingDownBrokerIds.contains(replica))
    //构造leaderAndIsr  加上 zkVersion 和 leader_epoch
    val newLeaderAndIsrOpt = leaderOpt.map(leader => leaderAndIsr.newLeaderAndIsr(leader, newIsr))
    ElectionResult(partition, newLeaderAndIsrOpt, liveOrShuttingDownReplicas)
  }

  /**
   * Elect leaders for partitions whose current leaders are shutting down.
   *
   * @param controllerContext Context with the current state of the cluster
   * @param leaderAndIsrs A sequence of tuples representing the partitions that need election
   *                                     and their respective leader/ISR states
   *
   * @return The election results
   */
  def leaderForControlledShutdown(controllerContext: ControllerContext,
                                  leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)]): Seq[ElectionResult] = {
    //获取需要关闭Broker的Id
    val shuttingDownBrokerIds = controllerContext.shuttingDownBrokerIds.toSet
    // 根据策略选出leader，返回 Seq ElectionResult
    leaderAndIsrs.map { case (partition, leaderAndIsr) =>
      leaderForControlledShutdown(partition, leaderAndIsr, shuttingDownBrokerIds, controllerContext)
    }
  }
}
