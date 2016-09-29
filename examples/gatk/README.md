### Disclaimer

This is not an official Google product.

## GATK with DockerFlow

This example uses Dockerflow to implement a popular bioinformatics workflow, the
[Broad Institute GATK](http://broadinstitute.org/gatk) Best Practices pipeline for human whole
genome sequencing.

This example shows how to:

* Define a complex, real-world workflow in [YAML](http://yaml.org)
* Define the same workflow in Java
* Pass parameters to tasks
* Pass an output file from one task as an input to the next
* Scatter by input file or lines of a file
* Gather outputs
* Branch a graph to run steps in parallel

Take a look at `gatk-workflow.yaml` and `GatkPairedSingleSample.java` for details.

**In order to run this example, you must agree to the [GATK End User License Agreement](https://software.broadinstitute.org/gatk/download/licensing), including the attribution requirement.**

## Prerequisites

1. Build the [Dockerflow](/googlegenomics/dockerflow) jar file

## Running the example

There are two ways to run the example: from the YAML file, and from Java. It's entirely up to you
if you prefer to write Java code or YAML. The Java is a bit more compact. The YAML doesn't need to be
compiled.

### Running from a YAML definition

To run the example using the YAML file:

	java -jar PATH/TO/dockerflow*dependencies.jar \
	    --project=MY-PROJECT \
	    --workflow-file=gatk-workflow.yaml \
	    --args-file=gatk-args.yaml \
	    --workspace=gs://MY-BUCKET/MY-PATH \
	    --preemptible

Replace `PATH/TO` with the path to your jar file.
Set `MY-BUCKET` and `MY-PATH` to a bucket and path that you'd like to use to store output
files, working files, and logs. Set `MY-PROJECT` to your cloud project name.

Things to note: the args file contains the paths to the input files in Google Cloud Storage. You
can change the paths to point to your own data.

The `workspace` is the location where your logs and output files will go.

By setting `--preemptible`, your workflow will run with preemptible VMs, which can save money.
The downside is they're more likely to be terminated and need retries. Fortunately, DockerFlow
automatically retries failed steps for you.

### Running from a Java definition

To run the same example using the Java definition rather than YAML, first compile the class:

	javac -cp PATH/TO/dockerflow*dependencies.jar GatkPairedSingleSample.java

Then run the workflow from the Java class:

	java -jar PATH/TO/dockerflow*dependencies.jar \
	    --project=MY-PROJECT \
	    --workflow-class=GatkPairedSingleSample \
	    --args-file=gatk-params.yaml \
	    --workspace=gs://MY-BUCKET/MY-PATH \
	    --preemptible

Set `MY-BUCKET` and `MY-PATH` to a bucket and path that you'd like to use to store output
files, working files, and logs. Set `MY-PROJECT` to your cloud project name.

### Troubleshooting

To see detailed log messages during workflow execution, you can run locally by setting
`--runner=DirectPipelineRunner`.

To do a dry run, run with the `--test` flag. (Unfortunately, this will fail for GATK at the couple
of places where workflow stages depend upon the output of earlier stages. You can create dummy
files and store them in the expected locations.)

To resume a workflow where it left off, run with `--resume`. The workflow will start from the
beginning, and if all output files from a task already exist in Google Cloud Storage, the workflow
will skip ahead to the next task.

## Next steps

* Write your own pipeline
* Contribute to DockerFlow
