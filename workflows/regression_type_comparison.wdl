version 1.0

import "../../ukbiobank/workflow/gwas_wdl/gwas_tasks.wdl"
import "../../ukbiobank/workflow/expanse_wdl/expanse_files.wdl"

workflow gwas {

  call expanse_files.files

  Array[String] types = ['linear', 'logistic', 'firth']
  Array[Int] region_lens = [1000000000, 1000000, 100000]
  Array[String] times_per_region = ["48h", "14h", "14h"]
  Array[File] inputs = [
    "workflows/temp/combined_q4.plink_pheno_data.tab",
    "workflows/temp/combined_q4_binarized.plink_pheno_data.tab", 
    "workflows/temp/combined_q4_binarized.plink_pheno_data.tab",
  ]
  Array[File] dummy_str_gwas = [
    "workflows/temp/dummy_str_gwas.tab",
    "workflows/temp/dummy_str_gwas_binarized.tab",
    "workflows/temp/dummy_str_gwas_binarized.tab",
  ]
  Array[String] pheno_names = ['audit_combined_q4', 'audit_combined_q4_binarized', 'audit_combined_q4_binarized']

  scatter (num in range(length(types))) {
    call gwas_tasks.snp_association_regions { input :
      mfis = ["../ukbiobank/array_imputed/ukb_mfi_chr4_v3.txt"],
      region_len = region_lens[num]
    }

    scatter (snp_association_region in snp_association_regions.out_tsv) {
      Int snp_chrom = 4
      Int snp_start = snp_association_region[1]
      Int snp_end = snp_association_region[2]

      Int snp_chrom_minus_one = snp_chrom - 1

      call gwas_tasks.plink_snp_association as regional_plink_snp_association { input :
        script_dir = "../ukbiobank",
        plink_command = "plink2",
        imputed_snp_p_file = files.imputed_snp_pfiles[snp_chrom_minus_one],
        pheno_data = inputs[num],
        chrom = snp_chrom,
        start = snp_start,
        end = snp_end,
        phenotype_name = pheno_names[num],
        binary_type = types[num],
        time = times_per_region[num]
      }
    }

    call gwas_tasks.concatenate_tsvs as plink_snp_association { input :
      tsvs = regional_plink_snp_association.data,
      out = "~{pheno_names[num]}_~{types[num]}_chr4_snp_gwas"
    }

    call gwas_tasks.qq_plot as snp_qq_plot_ { input :
      script_dir = "../ukbiobank",
      results_tab = plink_snp_association.tsv,
      p_val_col = 'P',
      phenotype_name = pheno_names[num],
      variant_type = 'SNP',
      out_name = "~{pheno_names[num]}_~{types[num]}_chr4_snp_qq_plot",
      null_values = 'NA'
    }

    call gwas_tasks.overview_manhattan as overview_manhattan_ { input :
      script_dir = "../ukbiobank",
      phenotype_name = pheno_names[num],
      chr_lens = files.chr_lens,
      str_gwas_results = dummy_str_gwas[num],
      snp_gwas_results = plink_snp_association.tsv,
      ext = "png",
      prefix = "~{pheno_names[num]}_~{types[num]}_chr4_snp_",
      binary_type = types[num]
    }
  }

  output {
    Array[File] plink_snp_gwas = plink_snp_association.tsv
    Array[File] snp_qq_plot = snp_qq_plot_.plot
    Array[File] overview_manhattan = overview_manhattan_.plot
  }
}
