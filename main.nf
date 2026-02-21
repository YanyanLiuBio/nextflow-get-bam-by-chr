nextflow.enable.dsl=2

include { get_chr } from './modules/get_chr.nf'

workflow {

    // Resolve S3 base path
    def path_s3 = params.dev ? "seqwell-dev/analysis" : "seqwell-analysis"

    bam_ch = Channel
        .fromPath("s3://seqwell-analysis/${params.run}/${params.analysis}/bam/*.md.bam")
        .filter { !it.name.endsWith('.bai') }
        .map { bam ->
            def sample_id = bam.baseName.tokenize(".")[0]
            tuple(sample_id, bam)
        }
        .take(params.dev ? params.number_of_inputs : -1)

    bam_index_chr_ch = get_chr(bam_ch)

    bam_index_chr_ch.view()
}
