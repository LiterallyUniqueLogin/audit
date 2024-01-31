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

workflow gwass {
   Array[String] target_npy_names = [
     "combined_q4",
     "combined_q4_binarized"
   ]

   Array[String] covar_files = [
     "combined_covar_names",
     "combined_covar_names"
   ]

   Array[Boolean] is_binary = [
     false,
     true
   ]

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
    }

    scatter (file in flatten([
      gwas.pheno_data,
      [
        gwas.my_str_gwas,
        gwas.plink_snp_gwas,
        gwas.peaks,
        gwas.peaks_readme,
        gwas.finemapping_regions,
        gwas.finemapping_regions_readme,
        gwas.overview_manhattan
      ]
    ])) {
      call move_file_to_dir { input:
        file = file,
        dir_name = "~{target_npy_names[idx]}"
      }
    }
  }

  output {
    Array[Array[File]] out = move_file_to_dir.out
  }
}
