version 1.0

import "../../ukbiobank/workflow/expanse_wdl/expanse_files.wdl"
import "../../ukbiobank/workflow/finemapping_wdl/susie_sum_stats_one_region_workflow.wdl"

task move_files_to_dir {
  input {
    Array[File] files
    String dir_name
  }

  command <<<
    mkdir -p ~{dir_name}
    for file in ~{sep=" " files} ; do
      echo ~{dir_name}/$(basename $file)
      mv $file ~{dir_name}
    done
  >>>

  output {
    Array[File] out = read_lines(stdout()) #"~{dir_name}/~{basename(file)}"
  }

  runtime {
    dx_timeout: "2h"
    memory: "2GB"
  }
}

workflow test_new_finemapping {
  call expanse_files.files

  call susie_sum_stats_one_region_workflow.susie_sum_stats_one_region { input :
    script_dir = "../ukbiobank",

    str_vcf = files.str_vcfs[10],
    imputed_snp_bgen = files.imputed_snp_bgens[10],
    snp_vars_to_filter_from_finemapping = files.snps_to_filter[10],

    phenotype_samples = "data/gwas_results/combined_q1/association_sample_list.samples",

    my_str_gwas = "data/gwas_results/combined_q1/white_brits_str_gwas.tab",
    plink_snp_gwas = "data/gwas_results/combined_q1/white_brits_snp_gwas.tab",

    phenotype_name = "audit_combined_q1",
    bounds = object {
      chrom: 11,
      start: 47296963,
      end: 47845518
    },

    all_samples_list = files.all_samples_list,

    is_binary = false
  }
  
  call move_files_to_dir { input:
    files = flatten([[
        susie_sum_stats_one_region.susie_output.subset.lbf,
        susie_sum_stats_one_region.susie_output.subset.lbf_variable,
        susie_sum_stats_one_region.susie_output.subset.sigma2,
        susie_sum_stats_one_region.susie_output.subset.V,
        susie_sum_stats_one_region.susie_output.subset.converged,
        susie_sum_stats_one_region.susie_output.subset.lfsr,
        susie_sum_stats_one_region.susie_output.subset.requested_coverage,
        susie_sum_stats_one_region.susie_output.subset.alpha,
        susie_sum_stats_one_region.susie_output.subset.colnames,
      ],
      susie_sum_stats_one_region.susie_output.CSs,
    ]),
    dir_name = 'combined_q1/finemapping_new'
  }

  output {
    Array[File] out = move_files_to_dir.out
  }
}
