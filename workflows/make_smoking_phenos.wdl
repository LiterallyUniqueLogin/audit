version 1.0

import '../../ukbiobank/workflow/gwas_wdl/gwas_tasks.wdl'
import '../../ukbiobank/workflow/expanse_wdl/expanse_tasks.wdl'

task binarize_smoker_touchscreen {
  input {
    File in_data
  }

  output {
    File out = 'binarized_smoker_touchscreen.npy'
  }

  command <<<
    python -c '
    import numpy as np
    arr = np.load("~{in_data}")
    arr = arr[(arr[:, 1] == 0) | (arr[:, 1] == 1), :]
    np.save("binarized_smoker_touchscreen.npy", arr)
    '
  >>>

  runtime {
    docker: "quay.io/thedevilinthedetails/work/python_data:v1.0"
    dx_timeout: "30m"
    memory: "10GB"
  }
}

workflow make_smoking_phenos {

  File fam_file = '../ukbiobank/microarray/ukb46122_cal_chr1_v2_s488176.fam'
  File qced_white_brits = '../ukbiobank/sample_qc/runs/white_brits/no_phenotype/combined.sample'
  String script_dir = "../ukbiobank"

  call expanse_tasks.extract_field as year_of_birth { input :
    script_dir = script_dir,
    id = 34
  }

  call expanse_tasks.extract_field as month_of_birth { input :
    script_dir = script_dir,
    id = 52
  }

  call expanse_tasks.extract_field as date_of_death { input :
    script_dir = script_dir,
    id = 40000
  }
  
  call expanse_tasks.extract_field as assessment_ages { input :
    script_dir = script_dir,
    id = 21003
  }

  call expanse_tasks.extract_field as pcs { input :
    script_dir = script_dir,
    id = 22009
  }

  call gwas_tasks.load_shared_covars { input:
    script_dir = '../ukbiobank',
    fam_file = fam_file,
    sc_pcs = pcs.data,
    sc_assessment_ages = assessment_ages.data,
    n_pcs = 8
  }

  call expanse_tasks.extract_field as is_smoker_touchscreen { input:
    script_dir = '../ukbiobank',
    id = 1239
  }

  call gwas_tasks.load_continuous_phenotype as is_smoker_touchscreen_and_covars { input :
    script_dir = '../ukbiobank',
    sc = is_smoker_touchscreen.data,
    qced_sample_list = qced_white_brits,
    categorical_covariate_names = [],
    categorical_covariate_scs = [],
    assessment_ages_npy = load_shared_covars.assessment_ages,
    prefix = "is_smoker_all_responses"
  }

  call binarize_smoker_touchscreen { input :
    in_data = is_smoker_touchscreen_and_covars.data
  }

  # can just use standard pipeline for this, but cache it
  call expanse_tasks.extract_field as smoking_amt_touchscreen { input:
    script_dir = '../ukbiobank',
    id = 3456
  }

  call expanse_tasks.extract_field as ICD10_inpatient_codes { input:
    script_dir = '../ukbiobank',
    id = 41270
  }

  call expanse_tasks.extract_field as ICD10_inpatient_dates { input:
    script_dir = '../ukbiobank',
    id = 41280
  }

# TODO implement
#  call get_F17_dates { input:
#    
#  }
#
#  call gwas_tasks.load_binary_phenotype as inpatient_F17 { input:
#    script_dir = '../ukbiobank',
#    sc = get_F17_dates.data,
#    qced_sample_list = qced_white_brits,
#    sc_year_of_birth = year_of_birth.data,
#    sc_month_of_birth = month_of_birth.data,
#    data_of_most_recent_first_occurrence_update = "2021-04-01" # is actually inpatient, not first occurrence, but still is the right date
#  }

  # can just use the standard pipeline for this, but cache it
  call expanse_tasks.extract_field as smoking_as_ICD10_conglomorate_date { input:
    script_dir = '../ukbiobank',
    id = 130868
  }

  call expanse_tasks.extract_field as smoking_as_ICD10_conglomorate_source { input :
    script_dir = '../ukbiobank',
    id = 130869
  }

#  output {
#    File smoker_touchscreen = binarize_smoker_touchscreen.data 
#    File smoker_touchscreen_covar_names = is_smoker_touchscreen_and_covars.covar_names
#    File smoker_inpatient = inpatient_F17.data
#    File smoker_inpatient_covar_names = inpatient_F17.covar_names
#  }
}
