version 1.0

import "../../ukbiobank/workflow/expanse_wdl/gwas.wdl"

task move_file_to_dir {
  input {
    File file
    String dir_name
  }

  command <<<
    mkdir ~{dir_name}
    mv ~{file} ~{dir_name}
  >>>

  output {
    File out = "~{dir_name}/~{basename(file)}"
  }

  runtime {
    dx_timeout: "2h"
    memory: "2GB"
  }
}

#workflow test {
#  input {
#    File out1 = "data/gwas_results/q1/readme.txt"
#    File out2 = "data/gwas_results/q2/readme.txt"
#  }
#
#  call move_file_to_dir as q1 { input : 
#    file = out1,
#    dir_name = "q1_test"
#  }
#
#  call move_file_to_dir as q2 { input : 
#    file = out2,
#    dir_name = "q2_test"
#  }
#
#  output {
#    Array[File] files = [q1.out, q2.out]
#  }
#}

workflow gwass {
   Array[String] target_npy_names = [
#      'q1',
#      'q2',
#      'q3',
      'q4',
#      'q5',
#      'q6',
#      'q7',
#      'q8',
#      'q9',
#      'q10'
#      'c',
#      'p',
#      't'
#      'c_log10',
#      'p_log10',
#      't_log10'
    ]

  scatter (target_npy_name in target_npy_names) {
    call gwas.gwas { input:
      script_dir = "../ukbiobank",
      phenotype_name = "audit_~{target_npy_name}",
      premade_pheno_npy = "data/formatted_data/audit_~{target_npy_name}.npy",
      premade_pheno_covar_names = "data/formatted_data/audit_covar_names.txt",
      premade_pheno_readme = "data/formatted_data/empty_readme.txt",
      transform = false,
    }

    scatter (file in flatten([
      gwas.pheno_data,
      gwas.ethnic_my_str_gwas,
      [
        gwas.my_str_gwas,
        gwas.plink_snp_gwas,
        gwas.peaks,
        gwas.peaks_readme,
        gwas.finemapping_regions,
        gwas.finemapping_regions_readme,
      ]
    ])) {
      call move_file_to_dir { input:
        file = file,
        dir_name = "~{target_npy_name}"
      }
    }
  }

  output {
    Array[Array[File]] out = move_file_to_dir.out
  }
}
