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

package dagr.core.tasksystem

////////////////////////////////////////////////////////////////////////////////
// Scatter gather pipelines.
////////////////////////////////////////////////////////////////////////////////

/** Useful traits and classes for implementing scatter gather pipelines. */
object ScatterGatherPipeline {
  /** Delegates the return of the output of a given task.  The `output` method should only be called *after* this
    * task has been run. This is typically ensured by using a [[Callback]].*/
  trait OutputTaskDelegator[Output] extends OutputTask[Output] {
    var outputTask: Option[OutputTask[Output]] = None
    override def output: Output = outputTask.get.output
  }

  /** A task that creates the gather task when getTasks is called so that we guarantee that the inputs are populated
    * in each input task prior to gather task creation.  We also facilitate the return of the gather task's output.
    * This task is set to be dependent on all input tasks during the construction of this task. */
  class LazyOutputTaskDelegator[Output]
  (
    val inputTasks         : Iterable[OutputTask[Output]],
    val generateGatherTask : Iterable[Output] => GatherTask[Output]
  ) extends OutputTaskDelegator[Output] {

    inputTasks.foreach(_ ==> this)

    override def getTasks: Traversable[_ <: Task] = {
      outputTask = Some(generateGatherTask(inputTasks.map(_.output)))
      List(outputTask.get)
    }
  }

  /** Companion object to ScatterGatherFrameworkAdaptor that holds classes to help implement the adaptor */
  object ScatterGatherPipelineAdaptor {
    abstract class SimpleSplitInputTask[Input, Intermediate]
      extends SimpleInJvmTask
      with SplitInputTask[Input, Intermediate]

    abstract class SimpleScatterTask[Intermediate, Output]
      extends SimpleInJvmTask
      with ScatterTask[Intermediate, Output]

    abstract class SimpleGatherTask[Output]
      extends SimpleInJvmTask
      with GatherTask[Output]
  }

  /** A class to create a [[ScatterGatherPipeline]] from supplied methods.  This will create tasks to wrap and
    * execute each method in the correct order. This also allows other traits to mixed in as well. */
  class ScatterGatherPipelineAdaptor[Input, Intermediate, Output]
  (
      in: Input,
      toIntermediates: Input => Iterable[Intermediate],
      toOutput: Intermediate => Output,
      toFinalOutput: Iterable[Output] => Output
  ) extends ScatterGatherPipeline[Input, Intermediate, Output] {
    import ScatterGatherPipelineAdaptor._
    def input: Input = in
    def splitInputTask(input: Input) = new SimpleSplitInputTask[Input, Intermediate] {
      override def run(): Unit = _inputs = Option(toIntermediates(input))
    }
    def scatterTask(intermediate: Intermediate): ScatterTask[Intermediate, Output] =
      new SimpleScatterTask[Intermediate, Output] {
        override def run(): Unit = _output = Option(toOutput(intermediate))
      }
    def gatherTask(outputs: Iterable[Output]): GatherTask[Output] = new SimpleGatherTask[Output] {
      private var _output: Option[Output] = None
      override def output: Output = _output.get
      override def run(): Unit = _output = Option(toFinalOutput(outputs))
    }
  }

  /** Creates a ScatterGatherPipeline from the supplied methods */
  def apply[Input, Intermediate, Output]
  (
    in: Input,
    toIntermediates: Input => Iterable[Intermediate],
    toOutput: Intermediate => Output,
    toFinalOutput: Iterable[Output] => Output
  ): ScatterGatherFramework[Input, Intermediate, Output] = {
    new ScatterGatherPipelineAdaptor[Input, Intermediate, Output](
      in=in,
      toIntermediates=toIntermediates,
      toOutput=toOutput,
      toFinalOutput=toFinalOutput
    )
  }
}

/** The base trait for scatter gather pipelines.  This trait wires together the various methods in the
  * scatter gather framework.  It also implements a simple single gather step to gather all the outputs of
  * the scatter steps.
  *
  * @tparam Input        the input type to the scatter gather.
  * @tparam Intermediate the input type to each scatter.
  * @tparam Output       the output type of hte scatter gather.
  */
trait ScatterGatherPipeline[Input, Intermediate, Output]
  extends ScatterGatherFramework[Input, Intermediate, Output] {
  import ScatterGatherPipeline.{OutputTaskDelegator, LazyOutputTaskDelegator}

  /** The gather pipeline, which will return the final output. */
  private var gatherPipeline: Option[OutputPipeline] = None

  /** The output of the final gather task.  This method should only be called after the this pipeline has been run.
    * Use a [[Callback]] if you wish to have another task depend on this value. */
  override final def output: Output = gatherPipeline.get.output

  override final def build(): Unit = {
    val inputTask = splitInputTask(input)
    gatherPipeline = Some(new OutputPipeline)
    root ==> inputTask ==> gatherPipeline.get
    // set the intermediate inputs for the scatter steps to be updated after the input has been split. */
    Callbacks.connect(gatherPipeline.get, inputTask)((dest, src) => dest.intermediates = src.inputs)
  }

  /** Simple pipeline to wrap the creation of the scatter and gather tasks. This should depend on the `splitInputTask`
    * since the inputs to the scatter steps will not be available until the `splitInputTask` task completes. */
  private class OutputPipeline extends Pipeline with OutputTaskDelegator[Output] {
    /** The intermediate inputs are the inputs to the scatter steps.  A callback should be create to set these. */
    var intermediates: Iterable[Intermediate] = Nil
    override def build(): Unit = {
      val tasks = scatterTasks(intermediates)
      tasks.foreach(root ==> _)
      outputTask = Some(finalOutputTask(tasks))
    }
  }

  /** Generates a single gather step that gathers all the scatters. */
  override protected def finalOutputTask(inputTasks: Iterable[ScatterTask[Intermediate,Output]]): OutputTask[Output] = {
    // do not *create* the gather step until the scatter steps have completed.
    val task = new LazyOutputTaskDelegator[Output](
      inputTasks=inputTasks,
      generateGatherTask=gatherTask
    )
    task
  }
}

/** Companion to MergingScatterGatherPipeline */
object MergingScatterGatherPipeline {
  import ScatterGatherPipeline._

  val DefaultMergeSize = 2

  /** Creates a MergingScatterGatherPipeline from the supplied methods */
  def apply[Input, Intermediate, Output]
  (
    in: Input,
    toIntermediates: Input => Iterable[Intermediate],
    toOutput: Intermediate => Output,
    toFinalOutput: Iterable[Output] => Output
  ): MergingScatterGatherPipeline[Input, Intermediate, Output] = {
    new ScatterGatherPipelineAdaptor[Input, Intermediate, Output](
      in=in,
      toIntermediates=toIntermediates,
      toOutput=toOutput,
      toFinalOutput=toFinalOutput
    ) with MergingScatterGatherPipeline[Input, Intermediate, Output]
  }
}

/** A scatter-gather pipeline that will perform a recursive merge gather (ala merge-sort).  Please note that
  * `gatherTask` must be able to be called multiple times for this to work.
  * */
trait MergingScatterGatherPipeline[Input, Intermediate, Output]
  extends ScatterGatherPipeline[Input, Intermediate, Output] {
  import ScatterGatherPipeline.LazyOutputTaskDelegator

  def mergeSize: Int = MergingScatterGatherPipeline.DefaultMergeSize

  /** Performs a recursive merge of the gather steps, grouping scatter steps into sub-sets based on the merge size,
    * merging each sub-set, and iterating until there is only one step. */
  override protected def finalOutputTask(inputTasks: Iterable[ScatterTask[Intermediate, Output]]): OutputTask[Output] = {
    if (inputTasks.size == 1) {
      inputTasks.head
    }
    else {
      var currentTasks: Iterable[OutputTask[Output]] = inputTasks
      while (1 < currentTasks.size) {
        currentTasks = currentTasks.sliding(mergeSize, mergeSize).map {
          tasks =>
            if (tasks.size > 1) {
              new LazyOutputTaskDelegator(inputTasks=tasks, generateGatherTask=gatherTask)
            }
            else {
              tasks.head
            }
        }.toList
      }
      currentTasks.head
    }
  }
}
