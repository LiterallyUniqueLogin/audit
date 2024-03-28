version 1.0

import "../../ukbiobank/workflow/expanse_wdl/expanse_files.wdl"
import "../../ukbiobank/workflow/finemapping_wdl/first_pass_finemapping_sum_stats_workflow.wdl"
import "../../ukbiobank/workflow/finemapping_wdl/finemapping_tasks.wdl"

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

workflow finemapping {
  Array[String] target_npy_names = [
    "combined_q4_binarized",
    #"combined_q7_binarized",
    "combined_q8_binarized",
    "combined_q10_binarized",
    "combined_p_binarized",
#    "combined_q2",
#    "combined_q3",
#    "combined_t_log10",
  ]

  Array[Boolean] is_binary = [
    true,
    true,
    true,
    true,
  ]

  # must be false if is_binary is false
  Array[Boolean] firth = [
    false,
    false,
    false,
    false,
  ]

  call expanse_files.files

  scatter (idx in range(length(target_npy_names))) {
    call first_pass_finemapping_sum_stats_workflow.first_pass_finemapping_sum_stats as first_pass_finemapping { input:
      script_dir = "../ukbiobank",
      finemap_command = "finemap",

      chr_lens = files.chr_lens,

      str_vcfs = files.str_vcfs,
      imputed_snp_bgens = files.imputed_snp_bgens,
      snp_vars_to_filter_from_finemapping = files.snps_to_filter,

      phenotype_name = "audit_~{target_npy_names[idx]}",

      all_samples_list = files.all_samples_list,
      phenotype_samples = "data/gwas_results/~{target_npy_names[idx]}/association_sample_list.samples",

      shared_covars = 'data/gwas_results/shared_covars_8_pcs.npy',
      transformed_phenotype_data = "data/gwas_results/~{target_npy_names[idx]}/white_brits_pheno.npy",

      my_str_gwas = "data/gwas_results/~{target_npy_names[idx]}/" + (if !is_binary[idx] then "white_brits_str_gwas.tab" else "white_brits_logistic_str_gwas_continuous_p_less_0.1.tab"),
      plink_snp_gwas = "data/gwas_results/~{target_npy_names[idx]}/" + (if !is_binary[idx] then "white_brits_snp_gwas.tab" else "white_brits_logistic_snp_gwas_continuous_p_less_0.1.tab"),

      is_binary = is_binary[idx],
#      firth = firth[idx],
    }
    
    scatter (finemap in first_pass_finemapping.finemap) {
      File snp_file = finemap.snp_file
      File log_sss = finemap.log_sss
      File config = finemap.config
    }

    scatter (susie in first_pass_finemapping.susie) {
      File lbf = susie.lbf
      File lbf_variable = susie.lbf_variable
      File sigma2 = susie.sigma2
      File V = susie.V
      File converged = susie.converged
      File lfsr = susie.lfsr
      File requested_coverage = susie.requested_coverage
      File alpha = susie.alpha
      File colnames = susie.colnames
    }

    call move_files_to_dir { input:
      files = flatten([
        [first_pass_finemapping.first_pass_df],
        flatten([
          snp_file,
          log_sss,
          config,
          lbf,
          lbf_variable,
          sigma2,
          V,
          converged,
          lfsr,
          requested_coverage,
          alpha,
          colnames,
          flatten(first_pass_finemapping.finemap_creds),
          flatten(first_pass_finemapping.susie_CSs),
        ])
      ]),
      dir_name = (if !firth[idx] then "~{target_npy_names[idx]}" else "~{target_npy_names[idx]}_firth") + '/finemapping'
    }
  }

  output {
    Array[Array[File]] out = move_files_to_dir.out
  }
}
