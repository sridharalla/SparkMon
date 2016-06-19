package org.apache.sparkmon

import org.apache.log4j.Logger
import org.apache.spark.scheduler.{SparkListenerTaskEnd, SparkListenerTaskStart, SparkListenerUnpersistRDD, _}

import scala.collection.mutable.{HashMap, HashSet}

case class JobMetrics(
    var jobGroup: String = "",
    var jobId: Int = -1,
    var submissionTime: Option[Long] = None,
    var completionTime: Option[Long] = None,
    var numStages: Int = 0,
    var numActiveStages: Int = 0,
    var numCompletedStages: Int = 0,
    var numTasks: Int = 0,
    var numActiveTasks: Int = 0,
    var numCompletedTasks: Int = 0
) {
  var timeTaken: Long = 0
  var stageIds: Seq[Int] = Seq.empty
}

case class JobStageMetrics(
    var stageId: Int = -1,
    var name: String = "",
    var numTasks: Int = 0,
    var submissionTime: Option[Long] = None,
    var completionTime: Option[Long] = None,
    var numActiveTasks: Int = 0,
    var numCompletedTasks: Int = 0
) {
  var timeTaken: Long = 0

}

class Listener extends SparkListener {

  val Log = Logger.getLogger("root")

  type JobId = Int
  type StageId = Int
  val jobIdToMetrics = new HashMap[JobId, JobMetrics]
  val stageIdToMetrics = new HashMap[StageId, JobStageMetrics]
  val stageIdToActiveJobIds = new HashMap[StageId, JobId]

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    // jobStart.
    val jobGroup = jobStart.properties.getProperty("spark.jobGroup.id")

    val jobMetrics = JobMetrics(jobGroup, jobStart.jobId, Option(jobStart.time), None,
      jobStart.stageIds.length)

    jobMetrics.stageIds = jobStart.stageIds
    jobStart.stageInfos.foreach(si => jobMetrics.numTasks += si.numTasks)

    jobIdToMetrics += (jobStart.jobId -> jobMetrics)
    for (stageId <- jobStart.stageIds) {
      stageIdToActiveJobIds.getOrElseUpdate(stageId, jobStart.jobId)
    }
    Log.info("onJobStart jobGroup:%s jobId:%d stages:%d tasks:%d jobsubmissionTime:%d".format(
      jobGroup, jobMetrics.jobId,
      jobMetrics.numStages, jobMetrics.numTasks, jobMetrics.submissionTime.get
    ))
    super.onJobStart(jobStart)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val jobMetrics = jobIdToMetrics(jobEnd.jobId)
    jobMetrics.completionTime = Option(jobEnd.time)
    jobMetrics.timeTaken = jobMetrics.completionTime.get - jobMetrics.submissionTime.get

    Log.info("""onJobEnd jobGroup:%s jobId:%d stages:%d completedStages:%d
      numTasks:%d completedTasks:%d jobsubmissionTime:%d jobsubmissionTime:%d timeTaken:%d""".format(
      jobMetrics.jobGroup, jobMetrics.jobId,
      jobMetrics.numStages, jobMetrics.numCompletedStages,
      jobMetrics.numTasks, jobMetrics.numCompletedTasks, jobMetrics.submissionTime.get,
      jobMetrics.completionTime.get, jobMetrics.timeTaken
    ))

    jobMetrics.stageIds.foreach(s => stageIdToMetrics.remove(s))

    jobIdToMetrics.remove(jobMetrics.jobId)
    super.onJobEnd(jobEnd)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val stageId = stageSubmitted.stageInfo.stageId
    val jobId = stageIdToActiveJobIds(stageId)

    val stageMetrics = JobStageMetrics(stageId, stageSubmitted.stageInfo.name,
      stageSubmitted.stageInfo.numTasks, stageSubmitted.stageInfo.submissionTime)
    stageIdToMetrics += (stageId -> stageMetrics)

    val jobMetrics = jobIdToMetrics(jobId)

    jobMetrics.numActiveStages += 1
    Log.info("""onStageSubmitted jobGroup:%s jobId:%d stageId:%d stage:[%s] numTasks:%d activeStages:%d
      completedstages:%d/%d jobsubmissionTime:%d timeSoFar:%d ms"""
      .format(
        jobMetrics.jobGroup,
        jobMetrics.jobId,
        stageId,
        stageMetrics.name,
        stageMetrics.numTasks,
        jobMetrics.numActiveStages,
        jobMetrics.numCompletedStages,
        jobMetrics.numStages, jobMetrics.submissionTime.get,
        System.currentTimeMillis() - jobMetrics.submissionTime.get
      ))
    super.onStageSubmitted(stageSubmitted)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageId = stageCompleted.stageInfo.stageId
    val jobId = stageIdToActiveJobIds(stageId)

    val stageMetrics = stageIdToMetrics(stageId)
    stageMetrics.completionTime = stageCompleted.stageInfo.completionTime
    val jobMetrics = jobIdToMetrics(jobId)

    jobMetrics.numActiveStages -= 1
    jobMetrics.numCompletedStages += 1
    Log.info("""onStageCompleted jobGroup:%s jobId:%d stageId:%d stage:[%s]
      completedstages:%d/%d completedTasks:%d/%d timeTaken:%d ms jobsubmissionTime:%d timeSoFar:%d ms"""
      .format(
        jobMetrics.jobGroup,
        jobMetrics.jobId,
        stageId,
        stageMetrics.name,
        jobMetrics.numCompletedStages,
        jobMetrics.numStages, stageMetrics.numCompletedTasks,
        stageMetrics.numTasks, stageMetrics.timeTaken, jobMetrics.submissionTime.get,
        System.currentTimeMillis() - jobMetrics.submissionTime.get
      ))
    super.onStageCompleted(stageCompleted)
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {

    val stageId = taskStart.stageId
    val jobId = stageIdToActiveJobIds(stageId)

    val stageMetrics = stageIdToMetrics(stageId)
    stageMetrics.numActiveTasks += 1
    val jobMetrics = jobIdToMetrics(jobId)

    jobMetrics.numActiveTasks += 1
    Log.info("""onTaskStart jobGroup:%s jobId:%d stageId:%d stage:[%s] activeStages:%d
      completedstages:%d/%d activeTasks:%d completedTasks:%d/%d jobsubmissionTime:%d timeSoFar:%d ms"""
      .format(
        jobMetrics.jobGroup,
        jobMetrics.jobId,
        stageId,
        stageMetrics.name,
        jobMetrics.numActiveStages,
        jobMetrics.numCompletedStages,
        jobMetrics.numStages, stageMetrics.numActiveTasks,
        stageMetrics.numCompletedTasks, stageMetrics.numTasks,
        jobMetrics.submissionTime.get,
        System.currentTimeMillis() - jobMetrics.submissionTime.get
      ))

    super.onTaskStart(taskStart)
  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {
    //    println("onTaskGettingResult")
    super.onTaskGettingResult(taskGettingResult)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val stageId = taskEnd.stageId
    val jobId = stageIdToActiveJobIds(stageId)

    val stageMetrics = stageIdToMetrics(stageId)
    stageMetrics.numActiveTasks -= 1
    stageMetrics.numCompletedTasks += 1

    val jobMetrics = jobIdToMetrics(jobId)

    jobMetrics.numActiveTasks -= 1
    jobMetrics.numCompletedTasks += 1
    Log.info("""onTaskEnd jobGroup:%s jobId:%d stageId:%d stage:[%s] activeStages:%d
      completedstages:%d/%d activeTasks:%d completedTasks:%d/%d jobsubmissionTime:%d timeSoFar:%d ms"""
      .format(
        jobMetrics.jobGroup,
        jobMetrics.jobId,
        stageId,
        stageMetrics.name,
        jobMetrics.numActiveStages,
        jobMetrics.numCompletedStages,
        jobMetrics.numStages, stageMetrics.numActiveTasks,
        stageMetrics.numCompletedTasks, stageMetrics.numTasks,
        jobMetrics.submissionTime.get,
        System.currentTimeMillis() - jobMetrics.submissionTime.get
      ))
    super.onTaskEnd(taskEnd)
  }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = super.onEnvironmentUpdate(environmentUpdate)

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = super.onBlockManagerAdded(blockManagerAdded)

  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = super.onBlockManagerRemoved(blockManagerRemoved)

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = super.onUnpersistRDD(unpersistRDD)

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = super.onApplicationStart(applicationStart)

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = super.onApplicationEnd(applicationEnd)

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = super.onExecutorMetricsUpdate(executorMetricsUpdate)

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    super.onExecutorAdded(executorAdded)
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    super.onExecutorRemoved(executorRemoved)
  }

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = super.onBlockUpdated(blockUpdated)
}
