/*
 * Copyright 2016 Google.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.IOException;
import java.util.Map;

import com.google.cloud.genomics.dockerflow.args.ArgsBuilder;
import com.google.cloud.genomics.dockerflow.args.WorkflowArgs;
import com.google.cloud.genomics.dockerflow.task.Task;
import com.google.cloud.genomics.dockerflow.task.TaskBuilder;
import com.google.cloud.genomics.dockerflow.workflow.Workflow;
import com.google.cloud.genomics.dockerflow.workflow.Workflow.Branch;
import com.google.cloud.genomics.dockerflow.workflow.Workflow.Steps;
import com.google.cloud.genomics.dockerflow.workflow.WorkflowDefn;

/**
 * As an example of running a real-world workflow with Pipelines API and
 * Dockerflow, this is a translation of the
 * <a href="https://github.com/broadinstitute/wdl/blob/develop/scripts/broad_pipelines">
 * GATK WDL file</a> from June 2016.
 */
public class GatkPairedSingleSample implements WorkflowDefn {
  static final String GATK_IMAGE = "broadinstitute/genomes-in-the-cloud:2.2.3-1469027018";

  // Get version of BWA
  static Task BwaVersion = TaskBuilder.named("BwaVersion")
      .docker(GATK_IMAGE)
      .script("/usr/gitc/bwa 2>&1 | grep -e '^Version' | sed 's/Version: //'")
      .build();

  // Read unmapped BAM, convert on-the-fly to FASTQ and stream to BWA MEM for alignment
  static Task SamToFastqAndBwaMem = TaskBuilder.named("SamToFastqAndBwaMem")
      .inputFile("input_bam").scatterBy("input_bam")
      .input("bwa_commandline", "${bwa_commandline}")
      .input("output_bam_basename")
      .inputFile("ref_fasta", "${ref_fasta}")
      .inputFile("ref_fasta_index", "${ref_fasta_index}")
      .inputFile("ref_alt", "${ref_alt}")
      .inputFile("ref_amb", "${ref_amb}")
      .inputFile("ref_ann", "${ref_ann}")
      .inputFile("ref_bwt", "${ref_bwt}")
      .inputFile("ref_pac", "${ref_pac}")
      .inputFile("ref_sa", "${ref_sa}")
      .outputFile("output_bam", "${output_bam_basename}.bam")
      .outputFile("bwa_stderr_log", "${output_bam_basename}.bwa.stderr.log")
      .preemptible(true)
      .diskSize("${flowcell_medium_disk}")
      .memory(14)
      .cpu(16)
      .docker(GATK_IMAGE)
      .script(
        "set -o pipefail\n" +
        "# set the bash variable needed for the command-line\n" +
        "# bash_ref_fasta=${ref_fasta}\n" + // moved from here in WDL to bwa CLI below
        "# if ref_alt has data in it, \n" +
        "if [ -s ${ref_alt} ]; then\n" +
        "  java -Xmx3000m -jar /usr/gitc/picard.jar \\\n" +
        "  SamToFastq \\\n" +
        "  INPUT=${input_bam} \\\n" +
        "  FASTQ=/dev/stdout \\\n" +
        "  INTERLEAVE=true \\\n" +
        "  CLIPPING_ATTRIBUTE=XT \\\n" +
        "  CLIPPING_ACTION=2 \\\n" +
        "  NON_PF=true |\\\n" +
        "  /usr/gitc/${bwa_commandline} ${ref_fasta} /dev/stdin -  2> >(tee ${bwa_stderr_log} >&2) | \\\n" +
        "  samtools view -1 - > ${output_bam} && \\\n" +
        "  grep -m1 \"read .* ALT contigs\" ${bwa_stderr_log} | \\\n" +
        "  grep -v \"read 0 ALT contigs\"\n" +
        "# else ref_alt is empty or could not be found\n" +
        "else\n" +
        "  exit 1;\n" +
        "fi")
      .build();

  // Merge original input uBAM file with BWA-aligned BAM file
  static Task MergeBamAlignment = TaskBuilder.named("MergeBamAlignment")
      .inputFile("unmapped_bam")
      .input("bwa_commandline", "${bwa_commandline}")
      .input("bwa_version")
      .inputFile("aligned_bam")
      .input("output_bam_basename")
      .inputFile("ref_fasta", "${ref_fasta}")
      .inputFile("ref_fasta_index", "${ref_fasta_index}")
      .inputFile("ref_dict", "${ref_dict}")
       .outputFile("output_bam", "${output_bam_basename}.bam")
      .memory(3.5)
      .preemptible(true)
      .diskSize("${flowcell_medium_disk}")
       .docker(GATK_IMAGE)
       .script(
        "# set the bash variable needed for the command-line\n" +
        "bash_ref_fasta=${ref_fasta}\n" +
        "java -Xmx3000m -jar /usr/gitc/picard.jar \\\n" +
        "  MergeBamAlignment \\\n" +
        "  VALIDATION_STRINGENCY=SILENT \\\n" +
        "  EXPECTED_ORIENTATIONS=FR \\\n" +
        "  ATTRIBUTES_TO_RETAIN=X0 \\\n" +
        "  ALIGNED_BAM=${aligned_bam} \\\n" +
        "  UNMAPPED_BAM=${unmapped_bam} \\\n" +
        "  OUTPUT=${output_bam} \\\n" +
        "  REFERENCE_SEQUENCE=${ref_fasta} \\\n" +
        "  PAIRED_RUN=true \\\n" +
        "  SORT_ORDER=\"unsorted\" \\\n" +
        "  IS_BISULFITE_SEQUENCE=false \\\n" +
        "  ALIGNED_READS_ONLY=false \\\n" +
        "  CLIP_ADAPTERS=false \\\n" +
        "  MAX_RECORDS_IN_RAM=2000000 \\\n" +
        "  ADD_MATE_CIGAR=true \\\n" +
        "  MAX_INSERTIONS_OR_DELETIONS=-1 \\\n" +
        "  PRIMARY_ALIGNMENT_STRATEGY=MostDistant \\\n" +
        "  PROGRAM_RECORD_ID=\"bwamem\" \\\n" +
        "  PROGRAM_GROUP_VERSION=\"${bwa_version}\" \\\n" +
        "  PROGRAM_GROUP_COMMAND_LINE=\"${bwa_commandline} ${ref_fasta}\" \\\n" +
        "  PROGRAM_GROUP_NAME=\"bwamem\" \\\n" +
        "  UNMAP_CONTAMINANT_READS=true")
      .build();

  // Sort BAM file by coordinate order and fix tag values for NM and UQ
  static Task SortAndFixReadGroupBam = TaskBuilder.named("SortAndFixReadGroupBam")
      .inputFile("input_bam")
      .input("output_bam_basename")
      .inputFile("ref_dict", "${ref_dict}")
      .inputFile("ref_fasta", "${ref_fasta}")
      .inputFile("ref_fasta_index", "${ref_fasta_index}")
      .outputFile("output_bam", "${output_bam_basename}.bam")
      .outputFile("output_bam_index", "${output_bam_basename}.bai")
      .outputFile("output_bam_md5",  "${output_bam_basename}.bam.md5")
      .memory(5)
      .preemptible(true)
      .diskSize("${flowcell_medium_disk}")
      .docker(GATK_IMAGE)
      .script(
        "java -Xmx4000m -jar /usr/gitc/picard.jar \\\n" +
        "  SortSam \\\n" +
        "  INPUT=${input_bam} \\\n" +
        "  OUTPUT=/dev/stdout \\\n" +
        "  SORT_ORDER=\"coordinate\" \\\n" +
        "  CREATE_INDEX=false \\\n" +
        "  CREATE_MD5_FILE=false | java -Xmx500m -jar /usr/gitc/picard.jar \\\n" +
        "  SetNmAndUqTags \\\n" +
        "  INPUT=/dev/stdin \\\n" +
        "  OUTPUT=${output_bam} \\\n" +
        "  CREATE_INDEX=true \\\n" +
        "  CREATE_MD5_FILE=true \\\n" +
        "  REFERENCE_SEQUENCE=${ref_fasta}")
      .input("pipeline_run", "${workflow.index}").gatherBy("pipeline_run")
      .build();

  // Mark duplicate reads to avoid counting non-independent observations
  static Task MarkDuplicates = TaskBuilder.named("MarkDuplicates")
      .inputFileArray("input_bams", " INPUT=")
      .input("output_bam_basename")
      .input("metrics_filename")
      .outputFile("output_bam", "${output_bam_basename}.bam")
      .outputFile("duplicate_metrics", "${metrics_filename}")
      .memory(7)
      .preemptible(true)
      .diskSize("${agg_large_disk}")
      .docker(GATK_IMAGE)
      .script(
        "java -Xmx4000m -jar /usr/gitc/picard.jar \\\n" +
        "  MarkDuplicates \\\n" +
        "  INPUT=${input_bams} \\\n" +
        "  OUTPUT=${output_bam} \\\n" +
        "  METRICS_FILE=${duplicate_metrics} \\\n" +
        "  VALIDATION_STRINGENCY=SILENT \\\n" +
        "  OPTICAL_DUPLICATE_PIXEL_DISTANCE=2500 \\\n" +
        "  ASSUME_SORT_ORDER=\"queryname\"\\\n" +
        "  CREATE_MD5_FILE=true")
      .build();

  static Task SortAndFixSampleBam = TaskBuilder.fromTask(SortAndFixReadGroupBam, "SortAndFixSampleBam")
      .gatherBy(null)
      .diskSize("${agg_large_disk}")
      .build();

  // Generate sets of intervals for scatter-gathering over chromosomes
  static Task CreateSequenceGroupingTSV = TaskBuilder.named("CreateSequenceGroupingTSV")
      .inputFile("ref_dict", "${ref_dict}")
      .memory(2)
      .preemptible(true)
      .docker("python:2.7")
      // Use python to create the Sequencing Groupings used for BQSR and
      //PrintReads Scatter.  It outputs to stdout
      // where it is parsed into a wdl Array[Array[String]]
      // e.g. [["1"], ["2"], ["3", "4"], ["5"], ["6", "7", "8"]]
      .script(
        "python <<CODE\n" +
        "with open(\"${ref_dict}\", \"r\") as ref_dict_file:\n" +
        "  sequence_tuple_list = []\n" +
        "  longest_sequence = 0\n" +
        "  for line in ref_dict_file:\n" +
        "    if line.startswith(\"@SQ\"):\n" +
        "      line_split = line.split(\"\\t\")\n" +
        "      # (Sequence_Name, Sequence_Length)\n" +
        "      sequence_tuple_list.append((line_split[1].split(\"SN:\")[1], int(line_split[2].split(\"LN:\")[1])))\n" +
        "  longest_sequence = sorted(sequence_tuple_list, key=lambda x: x[1], reverse=True)[0][1]\n" +

        "# We are adding this to the intervals because hg38 has contigs named with embedded colons and a bug in GATK strips off\n" +
        "# the last element after a :, so we add this as a sacrificial element.\n" +
        "hg38_protection_tag = \":1+\"\n" +
        "# initialize the tsv string with the first sequence\n" +
        "tsv_string = sequence_tuple_list[0][0] + hg38_protection_tag\n" +
        "temp_size = sequence_tuple_list[0][1]\n" +
        "for sequence_tuple in sequence_tuple_list[1:]:\n" +
        "  if temp_size + sequence_tuple[1] <= longest_sequence:\n" +
        "    temp_size += sequence_tuple[1]\n" +
        "    tsv_string += \"\\t\" + sequence_tuple[0] + hg38_protection_tag\n" +
        "  else:\n" +
        "    tsv_string += \"\\n\" + sequence_tuple[0] + hg38_protection_tag\n" +
        "    temp_size = sequence_tuple[1]\n" +

        "print tsv_string\n" +
        "CODE")
      .build();

  // Generate Base Quality Score Recalibration (BQSR) model
  static Task BaseRecalibrator = TaskBuilder.named("BaseRecalibrator")
      .inputFile("input_bam")
      .inputFile("input_bam_index")
      .input("recalibration_report_filename")
      .inputArray("sequence_group_interval", " -L ").scatterBy("sequence_group_interval")
      .inputFile("dbSNP_vcf", "${dbSNP_vcf}")
      .inputFile("dbSNP_vcf_index", "${dbSNP_vcf_index}")
      .inputFile("known_snps_sites_vcf", "${known_snps_sites_vcf}")
      .inputFile("known_snps_sites_vcf_index", "${known_snps_sites_vcf_index}")
      .inputFile("known_indels_sites_vcf", "${known_indels_sites_vcf}")
      .inputFileArray("known_indels_sites_vcf_index", " -knownSites ", "${known_indels_sites_vcf_index}")
      .inputFile("ref_dict", "${ref_dict}")
      .inputFile("ref_fasta", "${ref_fasta}")
      .inputFile("ref_fasta_index", "${ref_fasta_index}")
      .outputFile("recalibration_report", "${recalibration_report_filename}")
      .memory(6)
      .preemptible(true)
      .diskSize("${agg_small_disk}")
      .docker(GATK_IMAGE)
      .script(
        "java -XX:GCTimeLimit=50 -XX:GCHeapFreeLimit=10 -XX:+PrintFlagsFinal \\\n" +
        "  -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCDetails \\\n" +
        "  -Xloggc:gc_log.log -Dsamjdk.use_async_io=false -Xmx4000m \\\n" +
        "  -jar /usr/gitc/GATK4.jar \\\n" +
        "  BaseRecalibrator \\\n" +
        "  -R ${ref_fasta} \\\n" +
        "  -I ${input_bam} \\\n" +
        "  --useOriginalQualities \\\n" +
        "  -O ${recalibration_report} \\\n" +
        "  -knownSites ${dbSNP_vcf} \\\n" +
        "  -knownSites ${known_snps_sites_vcf} \\\n" +
        "  -knownSites ${known_indels_sites_vcf} \\\n" +
        "  -L ${sequence_group_interval}")
      .build();

  // Apply Base Quality Score Recalibration (BQSR) model
  static Task ApplyBQSR = TaskBuilder.named("ApplyBQSR")
      .inputFile("input_bam")
      .inputFile("input_bam_index")
      .input("output_bam_basename")
      .inputFile("recalibration_report")
      .inputArray("sequence_group_interval", " -L ")
      .inputFile("ref_dict", "${ref_dict}")
      .inputFile("ref_fasta", "${ref_fasta}")
      .inputFile("ref_fasta_index", "${ref_fasta_index}")
      .outputFile("recalibrated_bam", "${output_bam_basename}.bam")
      .outputFile("recalibrated_bam_checksum", "${output_bam_basename}.bam.md5")
      .memory(3.5)
      .preemptible(true)
      .diskSize("${agg_small_disk}")
      .docker(GATK_IMAGE)
      .script(
        "java -XX:+PrintFlagsFinal -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps \\\n" +
        "  -XX:+PrintGCDetails -Xloggc:gc_log.log -Dsamjdk.use_async_io=false \\\n" +
        "  -XX:GCTimeLimit=50 -XX:GCHeapFreeLimit=10 -Xmx3000m \\\n" +
        "  -jar /usr/gitc/GATK4.jar \\\n" +
        "  ApplyBQSR \\\n" +
        "  --createOutputBamMD5 \\\n" +
        "  --addOutputSAMProgramRecord \\\n" +
        "  -R ${ref_fasta} \\\n" +
        "  -I ${input_bam} \\\n" +
        "  --useOriginalQualities \\\n" +
        "  -O ${recalibrated_bam} \\\n" +
        "  -bqsr ${recalibration_report} \\\n" +
        "  -SQQ 10 -SQQ 20 -SQQ 30 -SQQ 40 \\\n" +
        "  --emit_original_quals \\\n" +
        "  -L ${sequence_group_interval}")
      .input("pipeline_run", "${workflow.index}").gatherBy("pipeline_run")
      .build();

  // Combine multiple recalibration tables from scattered BaseRecalibrator runs
  static Task GatherBqsrReports = TaskBuilder.named("GatherBqsrReports")
      .inputFileArray("input_bqsr_reports", " -I ")
      .input("output_report_filename")
      .outputFile("output_bqsr_report", "${output_report_filename}")
      .memory(3.5)
      .preemptible(true)
      .diskSize("${flowcell_small_disk}")
      .docker(GATK_IMAGE)
      .script(
        "java -Xmx3000m -jar /usr/gitc/GATK4.jar \\\n"+
        "  GatherBQSRReports \\\n"+
        "  -I ${input_bqsr_reports} \\\n"+
        "  -O ${output_bqsr_report}")
      .build();

  static Task ApplyBQSRToUnmappedReads = TaskBuilder.fromTask(ApplyBQSR, "ApplyBQSRToUnmappedReads")
      .diskSize("${agg_small_disk}")
      .build();

  // Combine multiple recalibrated BAM files from scattered ApplyRecalibration runs
  static Task GatherBamFiles = TaskBuilder.named("GatherBamFiles")
      .inputFileArray("input_bams", " INPUT=")
      .inputFile("input_unmapped_reads_bam")
      .input("output_bam_basename")
      .outputFile("output_bam", "${output_bam_basename}.bam")
      .outputFile("output_bam_index", "${output_bam_basename}.bai")
      .outputFile("output_bam_md5", "${output_bam_basename}.bam.md5")
      .memory(3)
      .preemptible(true)
      .diskSize("${agg_large_disk}")
      .docker(GATK_IMAGE)
      .script(
        "java -Xmx2000m -jar /usr/gitc/picard.jar \\\n" +
        "  GatherBamFiles \\\n" +
        "  INPUT=${input_bams} \\\n" +
        "  INPUT=${input_unmapped_reads_bam} \\\n" +
        "  OUTPUT=${output_bam} \\\n" +
        "  CREATE_INDEX=true \\\n" +
        "  CREATE_MD5_FILE=true")
      .build();

  // Convert BAM file to CRAM format
  static Task ConvertToCram = TaskBuilder.named("ConvertToCram")
      .inputFile("input_bam")
      .inputFile("ref_fasta", "${ref_fasta}")
      .inputFile("ref_fasta_index", "${ref_fasta_index}")
      .input("output_bam_basename")
      .outputFile("output_cram", "${output_bam_basename}.cram")
      .outputFile("output_cram_index", "${output_bam_basename}.crai")
      .memory(3)
      .diskSize("${agg_medium_disk}")
      .docker(GATK_IMAGE)
      // Note that we are not activating preemptible instances for this step yet,
      // but we should if it ends up being fairly quick
      .script(
        "samtools view -C -T ${ref_fasta} ${input_bam} | \\\n" +
        "tee ${output_cram} | \\\n" +
        "md5sum > ${output_cram}.md5 && \\\n" +
        "seq_cache_populate.pl -root ./ref/cache ${ref_fasta} && \\\n" +
        "REF_PATH=: REF_CACHE=./ref/cache/%2s/%2s/%s \\\n" +
        "samtools index ${output_cram} && \\\n" +
        "mv ${output_cram}.crai ${output_cram_index}")
      .build();

  // Call variants on a single sample with HaplotypeCaller to produce a GVCF
  static Task HaplotypeCaller = TaskBuilder.named("HaplotypeCaller")
      .inputFile("input_bam")
      .inputFile("input_bam_index")
      .inputFile("interval_list").scatterBy("interval_list")
      .input("gvcf_basename")
      .input("contamination", "0")
      .inputFile("ref_dict", "${ref_dict}")
      .inputFile("ref_fasta", "${ref_fasta}")
      .inputFile("ref_fasta_index", "${ref_fasta_index}")
      .outputFile("output_gvcf", "${gvcf_basename}.vcf.gz")
      .outputFile("output_gvcf_index", "${gvcf_basename}.vcf.gz.tbi")
      .memory(10)
      .preemptible(true)
      .diskSize("${agg_small_disk}")
      .docker(GATK_IMAGE)
      .script(
        "java -XX:GCTimeLimit=50 -XX:GCHeapFreeLimit=10 -Xmx8000m \\\n" +
        "  -jar /usr/gitc/GATK35.jar \\\n" +
        "  -T HaplotypeCaller \\\n" +
        "  -R ${ref_fasta} \\\n" +
        "  -o ${output_gvcf} \\\n" +
        "  -I ${input_bam} \\\n" +
        "  -L ${interval_list} \\\n" +
        "  -ERC GVCF \\\n" +
        "  --max_alternate_alleles 3 \\\n" +
        "  -variant_index_parameter 128000 \\\n" +
        "  -variant_index_type LINEAR \\\n" +
        "  -contamination ${contamination} \\\n" +
        "  --read_filter OverclippedRead")
      .input("pipeline_run", "${workflow.index}").gatherBy("pipeline_run")
      .build();

  // Combine multiple VCFs or GVCFs from scattered HaplotypeCaller runs
  static Task GatherVCFs = TaskBuilder.named("GatherVCFs")
      .inputFileArray("input_vcfs", " INPUT=")
      .inputFileArray("input_vcfs_indexes", " ")
      .input("output_vcf_name")
      .outputFile("output_vcf", "${output_vcf_name}")
      .outputFile("output_vcf_index", "${output_vcf_name}.tbi")
      .memory(3)
      .preemptible(true)
      .diskSize("${agg_small_disk}")
      .docker(GATK_IMAGE)
      // using MergeVcfs instead of GatherVcfs so we can create indices
      // WARNING  2015-10-28 15:01:48  GatherVcfs  Index creation not
      // currently supported when gathering block compressed VCFs.
      .script(
        "java -Xmx2g -jar /usr/gitc/picard.jar \\\n" +
        "MergeVcfs \\\n" +
        "INPUT=${input_vcfs} \\\n" +
        "OUTPUT=${output_vcf}")
      .build();

  // Declarations and defaults, in addition to the user-provided parameters file
  static WorkflowArgs workflowArgs = ArgsBuilder.of()
      .input("bwa_commandline", "bwa mem -K 100000000 -p -v 3 -t 16") // $bash_ref_fasta")
      .input("recalibrated_bam_basename", "${sample_name}.aligned.duplicates_marked.recalibrated")

      .input("SamToFastqAndBwaMem.input_bam", "${flowcell_unmapped_bams}")
      .input("SamToFastqAndBwaMem.output_bam_basename",
          "${= '${input_bam}'.replace(/gs:.*\\//, '').replace(/.bam/, '.unmerged'); }")

      .input("MergeBamAlignment.unmapped_bam", "${SamToFastqAndBwaMem.input_bam}")
      .inputFromFile("MergeBamAlignment.bwa_version", "${BwaVersion.task.stdout}")
      .input("MergeBamAlignment.aligned_bam", "${SamToFastqAndBwaMem.output_bam}")
      .input("MergeBamAlignment.output_bam_basename",
          "${= '${SamToFastqAndBwaMem.input_bam}'.replace(/gs:.*\\//, '').replace(/.bam/, '.aligned.unsorted'); }")

      .input("SortAndFixReadGroupBam.input_bam", "${MergeBamAlignment.output_bam}")
      .input("SortAndFixReadGroupBam.output_bam_basename",
          "${= '${SamToFastqAndBwaMem.input_bam}'.replace(/gs:.*\\//, '').replace(/.bam/, '.sorted'); }")

      .input("MarkDuplicates.input_bams", "${MergeBamAlignment.output_bam}")
      .input("MarkDuplicates.output_bam_basename", "${sample_name}.aligned.unsorted.duplicates_marked")
      .input("MarkDuplicates.metrics_filename", "${sample_name}.duplicate_metrics")

      .input("SortAndFixSampleBam.input_bam", "${MarkDuplicates.output_bam}")
      .input("SortAndFixSampleBam.output_bam_basename", "${sample_name}.aligned.duplicate_marked.sorted")

      .input("BaseRecalibrator.input_bam", "${SortAndFixSampleBam.output_bam}")
      .input("BaseRecalibrator.input_bam_index", "${SortAndFixSampleBam.output_bam_index}")
      .input("BaseRecalibrator.recalibration_report_filename", "${sample_name}.recal_data.csv")
      .inputFromFile("BaseRecalibrator.sequence_group_interval", "${CreateSequenceGroupingTSV.task.stdout}")

      .input("ApplyBQSR.input_bam", "${SortAndFixSampleBam.output_bam}")
      .input("ApplyBQSR.input_bam_index", "${SortAndFixSampleBam.output_bam_index}")
      .input("ApplyBQSR.output_bam_basename", "${recalibrated_bam_basename}")
      .input("ApplyBQSR.recalibration_report", "${BaseRecalibrator.recalibration_report}")
      .input("ApplyBQSR.sequence_group_interval", "${BaseRecalibrator.sequence_group_interval}")

      .input("GatherBqsrReports.input_bqsr_reports", "${BaseRecalibrator.recalibration_report}")
      .input("GatherBqsrReports.output_report_filename", "${sample_name}.recal_data.csv")

      .input("ApplyBQSRToUnmappedReads.input_bam", "${SortAndFixSampleBam.output_bam}")
      .input("ApplyBQSRToUnmappedReads.input_bam_index", "${SortAndFixSampleBam.output_bam_index}")
      .input("ApplyBQSRToUnmappedReads.output_bam_basename", "${recalibrated_bam_basename}")
      .input("ApplyBQSRToUnmappedReads.recalibration_report", "${GatherBqsrReports.output_bqsr_report}")
      .input("ApplyBQSRToUnmappedReads.sequence_group_interval", "unmapped")

      .input("GatherBamFiles.input_bams", "${ApplyBQSR.recalibrated_bam}")
      .input("GatherBamFiles.input_unmapped_reads_bam", "${ApplyBQSRToUnmappedReads.recalibrated_bam}")
      .input("GatherBamFiles.output_bam_basename", "${sample_name}")

      .input("ConvertToCram.input_bam", "${GatherBamFiles.output_bam}")
      .input("ConvertToCram.output_bam_basename", "${sample_name}")

      .input("HaplotypeCaller.input_bam", "${GatherBamFiles.output_bam}")
      .input("HaplotypeCaller.input_bam_index", "${GatherBamFiles.output_bam_index}")
      .input("HaplotypeCaller.interval_list", "${scattered_calling_intervals}")
      .input("HaplotypeCaller.gvcf_basename", "${sample_name}")

      .input("GatherVCFs.input_vcfs", "${HaplotypeCaller.output_gvcf}")
      .input("GatherVCFs.input_vcfs_indexes", "${HaplotypeCaller.output_gvcf_index}")
      .input("GatherVCFs.output_vcf_name", "${final_gvcf_name}")

      .output("duplicate_metrics", "${MarkDuplicates.duplicate_metrics}")
      .output("bqsr_report", "${GatherBqsrReports.output_bqsr_report}")
      .output("cram", "${ConvertToCram.output_cram}")
      .output("cram_index", "${ConvertToCram.output_cram_index}")
      .output("vcf", "${GatherVCFs.output_vcf}")
      .output("vcf_index", "${GatherVCFs.output_vcf_index}")
      .build();

  @Override
  public Workflow createWorkflow(String[] args) throws IOException {
    return TaskBuilder.named(GatkPairedSingleSample.class.getSimpleName())
        .steps(
            Steps.of(
                Branch.of(
                    CreateSequenceGroupingTSV,
                    Steps.of(
                        BwaVersion,
                        SamToFastqAndBwaMem,
                        MergeBamAlignment,
                        SortAndFixReadGroupBam,
                        MarkDuplicates,
                        SortAndFixSampleBam)),
                BaseRecalibrator,
                ApplyBQSR,
                GatherBqsrReports,
                ApplyBQSRToUnmappedReads,
                GatherBamFiles,
                Branch.of(
                    ConvertToCram,
                    Steps.of(
                        HaplotypeCaller,
                        GatherVCFs))))
        .args(workflowArgs).build();
  }
}
