version 1.0

import "../../ukbiobank/workflow/expanse_wdl/gwas.wdl"
import "../../ukbiobank/workflow/expanse_wdl/expanse_files.wdl"
import "../../ukbiobank/workflow/gwas_wdl/gwas_tasks.wdl"

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
    Array[File] out = read_lines(stdout())
  }

  runtime {
    dx_timeout: "2h"
    memory: "2GB"
  }
}

task first_two_lines {
  input {
    File file
  }

  command <<<
    head -2 file > out.tab
  >>>

  output {
    File out = 'out.tab'
  }

  runtime {
    dx_timeout: "2h"
    memory: "2GB"
  }
}

workflow gwass {
  Array[String] target_npy_names = [
#    "combined_c",
#    "combined_c_log10",
#    "combined_p",
#    "combined_p_log10",
#    "combined_t",
#    "combined_t_log10",
#    "combined_q1",
#    "combined_q2",
#    "combined_q3",
#    "combined_q4",
#    "combined_q5",
#    "combined_q6",
#    "combined_q7",
#    "combined_q8",
#    "combined_q9",
#    "combined_q10",
    "combined_p_binarized"
  ]

  Array[String] covar_files = [
#    "combined_covar_names",
#    "combined_covar_names",
#    "combined_covar_names",
#    "combined_covar_names",
#    "combined_covar_names",
#    "combined_covar_names",
#    "combined_covar_names",
#    "combined_covar_names",
#    "combined_covar_names",
#    "combined_covar_names",
#    "combined_covar_names",
#    "combined_covar_names",
#    "combined_covar_names",
#    "combined_covar_names",
#    "combined_covar_names",
#    "combined_covar_names",
    "combined_covar_names",
  ]

  Array[Boolean] is_binary = [
#    false,
#    false,
#    false,
#    false,
#    false,
#    false,
#    false,
#    false,
#    false,
#    false,
#    false,
#    false,
#    false,
#    false,
#    false,
#    false,
    true
  ]

  # must be false if is_binary is false
  Array[Boolean] firth = [
#    false,
#    false,
#    false,
#    false,
#    false,
#    false,
#    false,
#    false,
#    false,
#    false,
#    false,
#    false,
#    false,
#    false,
#    false,
#    false,
    false
  ]

  call expanse_files.files

  scatter (idx in range(length(target_npy_names))) {
    call gwas.gwas { input:
      script_dir = "../ukbiobank",
      phenotype_name = "audit_~{target_npy_names[idx]}",
      premade_pheno_npy = "data/formatted_data/~{target_npy_names[idx]}.npy",
      premade_pheno_covar_names = "data/formatted_data/~{covar_files[idx]}.txt",
      premade_pheno_readme = "data/formatted_data/empty_readme.txt",
      transform = false,
      other_ethnicities = false,
      is_binary = is_binary[idx],
      firth = firth[idx],
      n_pcs = 8
    }

#    call first_two_lines as dummy_str_calls { input :
#      file = gwas.continuous_first_pass_str_gwas
#    }
#
#    call gwas_tasks.overview_manhattan { input :
#      script_dir = "../ukbiobank",
#      phenotype_name = "audit_~{target_npy_names[idx]}",
#      chr_lens = files.chr_lens,
#      str_gwas_results = dummy_str_calls.out,
#      snp_gwas_results = gwas.continuous_first_pass_snp_gwas,
#      ext = "png",
#      prefix = 'audit_~{target_npy_names[idx]}_continuous_snp_only_maf_more_0.05_info_more_0.9_'
#    }

    if (is_binary[idx]) {
      call move_files_to_dir as binary_move_files_to_dir { input :
        files = flatten([
          gwas.pheno_data,
          [
            gwas.my_str_gwas,
            gwas.plink_snp_gwas,
            gwas.peaks,
            gwas.peaks_readme,
            gwas.finemapping_regions,
            gwas.finemapping_regions_readme,
            gwas.overview_manhattan,
            gwas.snp_qq_plot,
            gwas.str_qq_plot,
          ],
          select_all([
            gwas.continuous_first_pass_str_gwas,
            gwas.continuous_first_pass_snp_gwas,
          ]),
        ]),
        dir_name = if !firth[idx] then "~{target_npy_names[idx]}" else "~{target_npy_names[idx]}_firth"
      }
    }
    if (!is_binary[idx]) {
      call move_files_to_dir as continuous_move_files_to_dir { input :
        files = flatten([
          gwas.pheno_data,
          [
            gwas.my_str_gwas,
            gwas.plink_snp_gwas,
            gwas.peaks,
            gwas.peaks_readme,
            gwas.finemapping_regions,
            gwas.finemapping_regions_readme,
            gwas.overview_manhattan,
            gwas.snp_qq_plot,
            gwas.str_qq_plot,
          ],
        ]),
        dir_name = "~{target_npy_names[idx]}"
      }
    }
    Array[File] moved_files = select_first([binary_move_files_to_dir.out, continuous_move_files_to_dir.out])
  }

  output {
    Array[Array[File]] out = moved_files
  }
}
