### Disclaimer

This is not an official Google product.

## Hello, World example

This is a "hello, world" example of Dockerflow, showing both YAML and Java versions.

## Prerequisites

1. Complete the [Dockerflow](https://github.com/googlegenomics/dockerflow#getting-started) Getting Started instructions.

## Running the example

There are two ways to run the example: from the YAML file, and from Java. It's entirely up to you
if you prefer to write Java code or YAML.

### Running from a YAML definition

To run the example using the YAML file:

	dockerflow --project=MY-PROJECT \
	    --workflow-file=hello-workflow.yaml \
	    --args-file=hello-args.yaml \
	    --workspace=gs://MY-BUCKET/MY-PATH

Replace `PATH/TO` with the path to your jar file.
Set `MY-BUCKET` and `MY-PATH` to a bucket and path that you'd like to use to store output
files, working files, and logs. Set `MY-PROJECT` to your cloud project name.

The output will be located at `gs://MY-BUCKET/MY-PATH/logs/Hello/task-stdout.log`.

### Running from Java

To run the same example using the Java definition rather than YAML, first compile the class:

	javac -cp PATH/TO/dockerflow*dependencies.jar HelloWorkflow.java

Then run the workflow from the Java class:

	dockerflow --project=MY-PROJECT \
	    --workflow-class=HelloWorkflow \
	    --args-file=hello-args.yaml \
	    --workspace=gs://MY-BUCKET/MY-PATH

Set `MY-BUCKET` and `MY-PATH` to a bucket and path that you'd like to use to store output
files, working files, and logs. Set `MY-PROJECT` to your cloud project name.

The output will be located at `gs://MY-BUCKET/MY-PATH/logs/Hello/task-stdout.log`.
