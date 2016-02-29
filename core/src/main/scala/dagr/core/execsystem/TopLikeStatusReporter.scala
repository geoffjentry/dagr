/*
 * The MIT License
 *
 * Copyright (c) 2016 Fulcrum Genomics LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package dagr.core.execsystem

import dagr.commons.util.BiMap
import dagr.commons.util.StringUtil._
import dagr.commons.util.TimeUtil._
import dagr.core.tasksystem.{InJvmTask, ProcessTask, Task, UnitTask}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.tools.jline_embedded.{Terminal => JLineTerminal, TerminalFactory}

/** Simple methods for a terminal */
object Terminal {

  /** Gets a new terminal for each call, in case the terminal is re-sized. */
  private def terminal: JLineTerminal = TerminalFactory.get()

  def width: Int = terminal.getWidth

  def height: Int = terminal.getHeight

  def supportsAnsi: Boolean = terminal.isAnsiSupported
}

/** ANSI Strings for cursor movements.
  *
  * See: http://www.tldp.org/HOWTO/Bash-Prompt-HOWTO/x361.html
  */
object CursorMovement {
  def positionCursor(line: Int, column: Int): String = s"\033[$line;${column}H"

  def moveUp(numLines: Int): String = s"\033[${numLines}A"

  def moveDown(numLines: Int): String = s"\033[${numLines}B"

  def moveRight(numColumns: Int): String = s"\033[${numColumns}C"

  def moveLeft(numColumns: Int): String = s"\033[${numColumns}D"

  def clearScreen: String = "\033[2J"

  def eraseToEOL: String = "\033[K"

}

/** Mix this trait into a [[dagr.core.execsystem.TaskManager]] to provide the `refresh()` method for a
  * top-like status interface. */
trait TopLikeStatusReporter {
  this: TaskManager =>

  /** The header for the report */
  private def reportHeader: List[String] = List(
    "ID", "NAME", "STATUS", "STATE", "CORES", "MEMORY",
    "SUBMISSION_DATE", "START_DATE", "END_DATE",
    "EXECUTION_TIME", "TOTAL_TIME", "ATTEMPT_INDEX"
  )

  /** Truncates the line if it is too long and appends "...", otherwise returns the line */
  private def truncateLine(line: String, maxColumns: Int): String = if (maxColumns < line.length) line.substring(0, line.length-3) + "..." else line

  /** Splits the string by newlines, and truncates each line if necessary.  Enforces a maximum number of lines. */
  private def wrap(output: String, maxLines: Int, maxColumns: Int): String = {
    var lines = output.split("\n")
    lines = if (maxLines <= lines.length) {
      lines.slice(0, maxLines-2) ++ List("...")
    }
    else {
      lines
    }
    lines.map(truncateLine(_, maxColumns)).mkString("\n")
  }

  /** A row for the report.  Each row is a single task. This should match the column headers returned by [[reportHeader]]. */
  private def reportRow(taskInfo: TaskExecutionInfo): List[String] = {
    // get the total execution time, and total time since submission
    val (executionTime: String, totalTime: String) = TaskExecutionInfo.getDurationSinceStartAndFormat(taskInfo)
    // The state of execution
    val graphNodeState = graphNodeStateFor(taskInfo.taskId).map(_.toString).getOrElse("NA")
    List(
      taskInfo.taskId.toString(),
      taskInfo.task.name,
      taskInfo.status.toString,
      graphNodeState,
      f"${taskInfo.resources.cores.value}%.2f",
      taskInfo.resources.memory.prettyString,
      timestampStringOrNA(taskInfo.submissionDate),
      timestampStringOrNA(taskInfo.startDate),
      timestampStringOrNA(taskInfo.endDate),
      executionTime,
      totalTime,
      taskInfo.attemptIndex
    ).map(_.toString)
  }

  /** Writes a delimited string of the status of all tasks managed to the console.
    *
    * @param print the method to use to write task status information, one line at a time.
    * @param delimiter the delimiter between entries in a row.
    */
  def refresh(print: String => Unit, delimiter: String = "  "): Unit = {
    val taskInfoMap: BiMap[Task, TaskExecutionInfo] = taskToInfoBiMapFor
    val taskManagerResources = getTaskManagerResources
    var numLinesLeft = Terminal.height

    // Erase the screen, line-by-line.
    for (i <- 0 until Terminal.height) {
      print(CursorMovement.positionCursor(i, 0))
      print(CursorMovement.eraseToEOL)
    }
    print(CursorMovement.positionCursor(0, 0))

    //////////////////////////////////////////////////////////////////////////////////////////////
    // Header row
    //////////////////////////////////////////////////////////////////////////////////////////////
    val header = new mutable.StringBuilder()

    val unitTasks = taskInfoMap.filter { case (task, taskInfo) => task.isInstanceOf[UnitTask]}.map { case (task, taskInfo) => taskInfo }
    val runningTasks: Map[UnitTask, ResourceSet] = runningTasksMap

    val systemCoresPct: Double  = 100.0 * runningTasks.values.map(_.cores.value).sum / taskManagerResources.cores.value
    val systemMemoryPct: Double = 100.0 * runningTasks.filterKeys(_.isInstanceOf[ProcessTask]).values.map(_.memory.value).sum / taskManagerResources.systemMemory.value
    val jvmMemoryPct: Double    = 100.0 * runningTasks.filterKeys(_.isInstanceOf[InJvmTask]).values.map(_.memory.value).sum / taskManagerResources.jvmMemory.value

    // NB: only considers unit tasks
    val numRunning    = runningTasks.size
    val numEligible   = readyTasksList.size
    val numFailed     = unitTasks.count(taskInfo => TaskStatus.isTaskFailed(taskInfo.status))
    val numDone       = unitTasks.count(taskInfo => TaskStatus.isTaskDone(taskInfo.status))
    val numIneligible = unitTasks.count(taskInfo => taskInfo.status == TaskStatus.UNKNOWN)

    header.append(s"Cores:         " + f"${taskManagerResources.cores.value}%6.1f" + ", " + f"$systemCoresPct%.1f" + "% Used\n")
    header.append(s"System Memory: " + String.format("%6s", taskManagerResources.systemMemory.prettyString) + ", " + f"$systemMemoryPct%.1f" + "% Used\n")
    header.append(s"JVM Memory:    " + String.format("%6s", taskManagerResources.jvmMemory.prettyString) + ", " + f"$jvmMemoryPct%.1f" + "% Used\n")
    header.append(s"Unit Tasks:    $numRunning Running, $numDone Done, $numFailed Failed, $numEligible Eligible, $numIneligible Ineligible")

    print(wrap(header.toString, maxLines=numLinesLeft, maxColumns=Terminal.width) + "\n")
    numLinesLeft -= header.count(_ == '\n') + 1

    //////////////////////////////////////////////////////////////////////////////////////////////
    // Write the task status table
    //////////////////////////////////////////////////////////////////////////////////////////////

    if (0 < numLinesLeft) { print("\n"); numLinesLeft -= 1 }

    // Create the task status table
    val infos: ListBuffer[TaskExecutionInfo] = new ListBuffer[TaskExecutionInfo]()

    // Get tasks that have started or have no unmet dependencies
    infos ++= taskInfoMap
      .values
      .toList
      .filter { taskInfo => taskInfo.status == TaskStatus.STARTED || this.graphNodeFor(taskInfo.taskId).get.state == GraphNodeState.NO_PREDECESSORS}

    // Add those that failed if we have more lines
    if (infos.size < numLinesLeft) {
      val moreInfos = taskInfoMap
        .values
        .toList
        .filter { taskInfo => TaskStatus.isTaskFailed(taskInfo.status) }
        .sortBy(taskInfo => taskInfo.taskId)
      if (moreInfos.size + infos.size <= numLinesLeft) infos ++= moreInfos
      else infos ++= moreInfos.slice(0, numLinesLeft - infos.size)
    }

    // Add those that completed if we have more lines
    if (infos.size < numLinesLeft) {
      val moreInfos = taskInfoMap
        .values
        .toList
        .filter { taskInfo =>  TaskStatus.isTaskDone(taskInfo.status, failedIsDone=false) }
        .sortBy(taskInfo => taskInfo.taskId)
      if (moreInfos.size + infos.size <= numLinesLeft) infos ++= moreInfos
      else infos ++= moreInfos.slice(0, numLinesLeft - infos.size)
    }

    // Now generate a row (line) per task info
    val taskStatusTable: ListBuffer[List[String]] = new ListBuffer[List[String]]()
    taskStatusTable += reportHeader
    taskStatusTable ++= infos
      .sortBy(taskInfo => taskInfo.taskId)
      .sortBy(taskInfo => this.graphNodeFor(taskInfo.taskId).get.state)
      .sortBy(taskInfo => taskInfo.status)
      .map(reportRow)

    if (1 < taskStatusTable.size) {
      print(wrap(columnIt(taskStatusTable.toList, delimiter), maxLines = numLinesLeft, maxColumns = Terminal.width) + "\n")
      numLinesLeft -= taskStatusTable.count(_ == '\n') + 1
    }
  }
}
