### Disclaimer

This is not an official Google product.

# Update

As of 11 Nov 2017, Dockerflow is no longer actively maintained and will not be enhanced with new
features.

For multi-step batch workflows consisting of Docker tasks, we now recommend running in the cloud
using:

*   [dsub](https://github.com/googlegenomics/dsub), a command-line batch submission tool

To run multi-step workflows with dsub, you can create a bash or python script with multiple dsub
calls. Execution graphs can be constructed using dsub's task dependency functionality.

For any Dockerflow functionality that is not satisfied by dsub, please
[file an issue](https://github.com/googlegenomics/dsub/issues) in the dsub repository.

# Dockerflow

Dockerflow makes it easy to run a multi-step workflow of Docker tasks using
[Google Cloud Dataflow](https://cloud.google.com/dataflow) for orchestration.
Docker steps are run using the [Pipelines API](https://cloud.google.com/genomics/v1alpha2/pipelines).

You can run Dockerflow from a shell on your laptop, and the job will run in 
Google Cloud Platform using Dataflow's fully managed service and web UI.

Dockerflow workflows can be defined in [YAML](http://yaml.org) files, or by writing
Java code. Examples of workflows defined in YAML can be found in

*   [examples](examples)
*   [src/test/resources](src/test/resources)

Examples of workflows defined in Java can be found in

*   [examples](examples)
*   [src/test/java/com/google/cloud/genomics/dockerflow/examples](src/test/java/com/google/cloud/genomics/dockerflow/examples)

You can run a batch of workflows at once by providing a CSV file with one row per
workflow to define the parameters.

## Why Dockerflow?

This project was created as a proof-of-concept that Dataflow can be used
for monitoring and management of directed acyclic graphs of command-line tools.

Dataflow and Docker complement each other nicely:

*   Dataflow provides graph optimization, a nice monitoring interface, retries,
    and other niceties.
*   Docker provides portability of the tools themselves, and there's a large
    library of packaged tools already available as Docker images.

While Dockerflow supports a simple YAML workflow definition, a similar approach
could be taken to implement a runner for one of the open standards like [Common
Workflow Language]
(https://github.com/common-workflow-language/common-workflow-language) or
[Workflow Definition Language](github.com/broadinstitute/wdl).

## Table of contents

*   [Prerequisites](#prerequisites)
*   [Getting started](#getting-started)
*   [Docker + Dataflow vs custom scripts](#docker-and-dataflow-vs-custom-scripts)
*   [Creating your own workflows](#creating-your-own-workflows)
    *   [Sequential workflows](#sequential-workflows)
    *   [Parallel workflows](#parallel-workflows)
    *   [Branching workflows](#branching-workflows)
*   [Testing](#testing)
*   [What next](#what-next)

## Prerequisites

1.  Sign up for a Google Cloud Platform account and [create a project]
    (https://console.cloud.google.com/project?).
2.  [Enable the APIs]
    (https://console.cloud.google.com/flows/enableapi?apiid=genomics,dataflow,storage_component,compute_component&redirect=https://console.cloud.google.com)
    for Cloud Dataflow, Google Genomics, Compute Engine and Cloud Storage.\
3.  [Install the Google Cloud SDK](https://cloud.google.com/sdk/) and run

        gcloud init
        gcloud auth login
        gcloud auth application-default login

## Getting started

Run the following steps on your laptop or local workstation:

1.  git clone this repository.

        git clone https://github.com/googlegenomics/dockerflow

2.  Build it with Maven.

        cd dockerflow
        mvn package -DskipTests

3. Set up the DOCKERFLOW_HOME environment.

        export DOCKERFLOW_HOME="$(pwd)"
        export PATH="${PATH}":"${DOCKERFLOW_HOME}/bin"
        chmod +x bin/*

4.  Run a sample workflow:

        dockerflow --project=MY-PROJECT \
            --workflow-file=src/test/resources/linear-graph.yaml \
            --workspace=gs://MY-BUCKET/MY-PATH \
            --input BASE_DIR=gs://MY-BUCKET/MY-PATH/MY-INPUT-FILE.txt
            --runner=DirectPipelineRunner

Set `MY-PROJECT` to your cloud project name, and set `MY-BUCKET` and `MY-PATH`
to your cloud bucket and folder. You'll need to have a text file in Cloud Storage
as well, here called `MY-INPUT-FILE.txt`. You can copy one from
[src/test/resources/input-one.txt](src/test/resources/input-one.txt):

        gsutil cp src/test/resources/input-one.txt gs://MY-BUCKET/MY-PATH/input-one.txt

The example will run Dataflow locally with the `DirectPipelineRunner`, for
orchestration. It will spin up VMs remotely in Google Cloud to run the
individual tasks in Docker. Execution of the local Dataflow runner will block
until the workflow completes. The DirectPipelineRunner is useful for
debugging, because you'll see all of the log messages output to your shell.

To run in your cloud project, and see the pretty Dataflow UI in Google Cloud
Console, you can remove the `--runner` option to use the default Dataflow runner.

## Docker and Dataflow vs custom scripts

How is Dataflow better than a shell script?

Dataflow provides:

*   **Complex workflow orchestration**: Dataflow supports arbitrary directed
acyclic graphs. The logic of branching, merging, parallelizing, and monitoring is
all handled automatically.
*   **Monitoring**:
[Dataflow's monitoring UI](https://cloud.google.com/dataflow/pipelines/dataflow-monitoring-intf)
shows you what jobs you've run and shows an execution graph with nice details.
*   **Debugging**: Dataflow keeps logs at each step, and you can view them directly
in the UI.
*   **Task retries**: Dataflow automatically retries failed steps.
Dockerflow adds support for preemptible VMs, rerunning failures on standard VM
instances.
*   **Parallelization**: Dataflow can run 100 tasks on 100 files and
keep track of them all for you, retrying any steps that failed.
*   **Optimization**: Dataflow optimizes the execution graph for your workflow.

Docker provides:

*   **Portability**: Tools packaged in Docker images can be run
anywhere Docker is supported.
*   **A library of pre-packaged tools**: The community has contributed a growing
library of popular tools.

## Creating your own workflows

The Dockerflow command-line expects a static workflow graph definition in YAML,
or the Java class name of a Java definition.

If you'd rather define workflows in code, you'll use the Java SDK. See

*   [src/test/java/com/google/cloud/genomics/dockerflow/examples](src/test/java/com/google/cloud/genomics/dockerflow/examples)

Everything that can be done with YAML can also be done (and more compactly) in
Java code. Java provides greater flexibility too.

The documentation below provides details for defining workflows in YAML. To
create a workflow, you define the tasks and execution graph. You can
define the tasks and execution graph in a single file, or your graph can
reference tasks that are defined in separate YAML files.

A workflow is a recursive format, meaning that a workflow can contain multiple
steps, and each of the steps can be a workflow.

## Hello, world

Dockerflow has lots of features for creating complex, real-world workflows.
The best way to get started with your own workflows is to look at the
[examples](examples). 

The [hello, world](examples/hello) example shows the most basic workflow
in both YAML and Java.

All of the advanced features can be seen in the more complex
[GATK](examples/gatk) example. Again, it offers both YAML and Java
versions. You'll see pretty much the full range of functionality.

## Testing

Workflows can be tricky to test and debug. Dataflow has a local runner that
makes it easy to fix the obvious bugs before running in your Google Cloud
Platform project.

To test locally, set `--runner=DirectPipelineRunner`. Now Dataflow will run on
your local computer rather than in the cloud. You'll be able to see all of the
log messages.

Two other flags are really useful for testing: `--test=true` and
`--resume=true`.

When you set `test` to true, you'll get a dry run of the pipeline. No calls to
the Pipelines API will be made. Instead, the code will print a log message and
continue. That lets you do a first sanity check before submitting and running on
the cloud. You can catch many errors, mismatched parameters, etc.

When you use the `resume` flag, Dockerflow will try to resume a failed pipeline
run. For example, suppose you're trying to get your 10-step pipeline to work. It
fails on step 6. You go into your YAML definition file, or edit your Java code.
Now you want to re-run the pipeline. However, it takes 1 hour to run steps 1-5.
That's a long time to wait. With `--resume=true`, Dockerflow will look to see if
the outputs of each step exist already, and if they do, it will print a log
message and proceed to the next step. That means it takes only seconds to skip
ahead to the failed step and try to rerun it.

## What next?

*   See the YAML examples in the [src/test/resources](src/test/resources) directory.
*   See the Java code examples in
    *    [examples](examples)
    *    [src/test/java/com/google/cloud/genomics/dockerflow/examples](src/test/java/com/google/cloud/genomics/dockerflow/examples)
*   Learn about the [Pipelines API]
    (https://cloud.google.com/genomics/v1alpha2/pipelines).
*   Read about [Dataflow](https://cloud.google.com/dataflow).
*   Write your own workflows!

## FAQ and Troubleshooting

### What if I want to run large batch jobs?

Google Cloud Platform has various quotas that affect how many VMs and IP addresses, and how much disk space you can get. Some tips:

* [Check and potentially increase quotas](https://console.cloud.google.com/compute/quotas)
* Consider listing all zones in the region or geography where your Cloud Storage bucket is (e.g., for standard buckets in the EU, use "eu-"; for regional buckets in US central, use "us-central-")
* The pipeline system will queue jobs until resources are available if quotas are exceeded
* Dockerflow will abort if any job fails. Use the '--abort=false' flag for different behavior.
