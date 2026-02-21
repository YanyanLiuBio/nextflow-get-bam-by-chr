process get_chr {

    tag "${pair_id}"

    publishDir path: "s3://${path_s3}/${params.run}/glimpse_impute/${params.analysis}/bam_by_chr/${params.chr}",
               mode: 'copy'

    input:
    tuple val(pair_id), path(bam)

    output:
    tuple val(pair_id),
          path("${pair_id}.${params.chr}.bam"),
          path("${pair_id}.${params.chr}.bam.bai")

    """
    samtools index $bam
    samtools view -b $bam ${params.chr} > ${pair_id}.${params.chr}.bam
    samtools index ${pair_id}.${params.chr}.bam
    """
}
