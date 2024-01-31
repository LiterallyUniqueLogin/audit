version 1.0

import "../../ukbiobank/workflow/expanse_wdl/expanse_files.wdl"
import "../../ukbiobank/workflow/gwas_wdl/gwas_tasks.wdl"

workflow test_manhattan {
  call expanse_files.files

  call gwas_tasks.overview_manhattan { input:
    script_dir = "../ukbiobank",
    phenotype_name = "audit_c",
    chr_lens = files.chr_lens,
    str_gwas_results = "cromwell-executions/gwass/9fb6343e-1949-40c0-b0b5-b3372db10988/call-gwas/shard-0/gwas/2b51f26e-aba1-4ba8-a084-f85bbdf7791d/call-gwas/gwas/18423dc5-e3eb-4e7c-bf50-4529f10826b1/call-overview_manhattan_/inputs/895144261/white_brits_str_gwas.tab",
    snp_gwas_results = "cromwell-executions/gwass/9fb6343e-1949-40c0-b0b5-b3372db10988/call-gwas/shard-0/gwas/2b51f26e-aba1-4ba8-a084-f85bbdf7791d/call-gwas/gwas/18423dc5-e3eb-4e7c-bf50-4529f10826b1/call-overview_manhattan_/inputs/-831843365/white_brits_snp_gwas.tab",
    peaks = "cromwell-executions/gwass/9fb6343e-1949-40c0-b0b5-b3372db10988/call-gwas/shard-0/gwas/2b51f26e-aba1-4ba8-a084-f85bbdf7791d/call-gwas/gwas/18423dc5-e3eb-4e7c-bf50-4529f10826b1/call-overview_manhattan_/inputs/-116891304/peaks.tab",
    ext = "png"
  }

  output {
    File plot = overview_manhattan.plot
  }
}
