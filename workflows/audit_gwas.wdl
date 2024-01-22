version 1.0

import "extract_audit_scores.wdl"
import "../../ukbiobank/workflow/expanse_wdl/gwas.wdl"

task collect_phenos {

  input {
    String audit_script_dir = '.'
    File script = '~{audit_script_dir}/make_phenotypes.py'
    File year_of_birth_csv
    File month_of_birth_csv
    File audit_completion_date_csv
    Array[File] audit_question_csvs
  }

  command <<<
    python ~{script} \
      ~{year_of_birth_csv} \
      ~{month_of_birth_csv} \
      ~{audit_completion_date_csv} \
      ~{sep=" " audit_question_csvs}
  >>>

  output {
    File full_df = 'full_audit.tab'
    Array[File] audit_q_npys = [
      'audit_q1.npy',
      'audit_q2.npy',
      'audit_q3.npy',
      'audit_q4.npy',
      'audit_q5.npy',
      'audit_q6.npy',
      'audit_q7.npy',
      'audit_q8.npy',
      'audit_q9.npy',
      'audit_q10.npy'
    ]
    Array[File] audit_sum_npys = [
      'audit_c.npy',
      'audit_p.npy',
      'audit_t.npy'
    ] 
    Array[File] audit_log_sum_npys = [
      'audit_c_log10.npy',
      'audit_p_log10.npy',
      'audit_t_log10.npy'
    ]
  }

  runtime {
    dx_timeout: '30m'
    memory: '4GB'
    shortTask: true
  }
}

workflow gwas {
#  call extract_audit_scores
#
#  call collect_phenos { input :
#    year_of_birth_csv = extract_audit_scores.sc_year_of_birth,
#    month_of_birth_csv = extract_audit_scores.sc_month_of_birth,
#    audit_completion_date_csv = extract_audit_scores.sc_original_audit_completion_date
#    audit_question_csvs = extract_audit_scores.sc_original_audit_qs
#  }

  call gwas.gwas { input:
    script_dir = "../ukbiobank",
    phenotype_name = 'audit_q2',
    premade_pheno_npy = "data/formatted_data/audit_q2.npy",
    premade_pheno_covar_names = "data/formatted_data/audit_covar_names.txt",
    premade_pheno_readme = "data/formatted_data/empty_readme.txt",
    transform = false,
  }

  output {
    Array[File] pheno_data = gwas.pheno_data

    File my_str_gwas = gwas.my_str_gwas
    File plink_snp_gwas = gwas.plink_snp_gwas
    File peaks = gwas.peaks
    File peaks_readme = gwas.peaks_readme
    File finemapping_regions = gwas.finemapping_regions
    File finemapping_regions_readme = gwas.finemapping_regions_readme
    Array[File] ethnic_my_str_gwas = gwas.ethnic_my_str_gwas
  }
}
