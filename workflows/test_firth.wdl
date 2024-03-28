version 1.0

import "../../ukbiobank/workflow/gwas_wdl/gwas_tasks.wdl"
import "../../ukbiobank/workflow/expanse_wdl/expanse_files.wdl"

workflow test_firth {

  call expanse_files.files

  call gwas_tasks.plink_snp_association { input :
    script_dir = "../ukbiobank",
    plink_command = "plink2",
    imputed_snp_p_file = files.imputed_snp_pfiles[20],
    pheno_data = "/expanse/projects/gymreklab/jmargoli/audit/cromwell-executions/gwass/d7f4486f-89cb-468b-80a7-9c093c8dad08/call-gwas/shard-0/gwas/2a882bca-9011-4c69-a735-5940777c2790/call-gwas/gwas/8b896998-9e30-4f8b-a26b-ec4e6f2a73cc/call-prep_plink_input/cacheCopy/execution/out.tab",
    chrom = 21,
    start = 0,
    end = 16360451,
    phenotype_name = "audit_combined_q4_binarized",
    binary_type = "firth",
    transformed = false
  }

  output {
    File data = plink_snp_association.data
    File log = plink_snp_association.log
  }
}
