version 1.0

import "../../ukbiobank/workflow/gwas_wdl/gwas_tasks.wdl"
import "../../ukbiobank/workflow/expanse_wdl/expanse_files.wdl"

workflow gwas {

  call expanse_files.files

  Array[Int] str_association_regions = [
    0, 
    10000000, 
    20000000, 
    30000000, 
    40000000, 
    50000000, 
    60000000, 
    70000000, 
    80000000, 
    90000000, 
    100000000, 
    110000000, 
    120000000, 
    130000000, 
    140000000, 
    150000000, 
    160000000, 
    170000000, 
    180000000, 
    190000000, 
  ]

  Int chrom = 4
  scatter (start in str_association_regions) {
    Int end = start + 10000000 - 1 

    Int chrom_minus_one = chrom - 1

    region bounds = {
      "chrom": chrom,
      "start": start,
      "end": end
    }

    call gwas_tasks.regional_my_str_gwas { input :
      script_dir = "../ukbiobank",
      str_vcf = files.str_vcfs[chrom_minus_one],
      shared_covars = "workflows/temp/shared_covars.npy",
      untransformed_phenotype = "workflows/temp/white_brits_q4_combined_binarized.npy",
      transformed_phenotype = "workflows/temp/white_brits_q4_combined_binarized.npy",
      all_samples_list = files.all_samples_list,
      is_binary = true,
      bounds = bounds,
      phenotype_name = "audit_combined_q4_binarized",
    }
  }

  call gwas_tasks.concatenate_tsvs as my_str_gwas_ { input :
    tsvs = regional_my_str_gwas.data,
    out = "white_brits_str_gwas"
  }

  call gwas_tasks.qq_plot as str_qq_plot_ { input :
    script_dir = "../ukbiobank",
    results_tab = my_str_gwas_.tsv,
    p_val_col = 'p_audit_combined_q4_binarized',
    phenotype_name = "audit_combined_q4_binarized",
    variant_type = 'STR',
    out_name = "audit_combined_q4_binarized_logistic_chr4_str_qq_plot",
  }

#    call gwas_tasks.overview_manhattan as overview_manhattan_ { input :
#      script_dir = "../ukbiobank",
#      phenotype_name = pheno_names[num],
#      chr_lens = files.chr_lens,
#      str_gwas_results = dummy_str_gwas[num],
#      snp_gwas_results = plink_snp_association.tsv,
#      ext = "png",
#      prefix = "~{pheno_names[num]}_~{types[num]}_chr4_snp_",
#      binary_type = types[num]
#    }

  output {
    File my_str_gwas = my_str_gwas_.tsv
    File str_qq_plot = str_qq_plot_.plot
    #Array[File] overview_manhattan = overview_manhattan_.plot
  }
}
