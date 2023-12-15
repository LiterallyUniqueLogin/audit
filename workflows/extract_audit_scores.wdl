version 1.0

import "../../ukbiobank/workflow/expanse_wdl/expanse_tasks.wdl"

workflow extract_audit_scores {

  Array[Int] original_audit_ids = [
    20414,
    20403,
    20416,
    20413,
    20407,
    20412,
    20409,
    20408,
    20411,
    20405
  ]

  call expanse_tasks.extract_field as original_audit_completion_date { input :
    script_dir = "../ukbiobank",
    id = 20400
  }

  scatter (original_audit_id in original_audit_ids) {
    call expanse_tasks.extract_field as original_audit_qs { input :
      script_dir = "../ukbiobank",
      id = original_audit_id
    }
    File sc_original_audit_qs_ = original_audit_qs.data
  }

  Array[Int] new_audit_ids = [
    29091,
    29092,
    29093,
    29094,
    29095,
    29096,
    29097,
    29098,
    29099,
    29100
  ]

  scatter (new_audit_id in new_audit_ids) {
    call expanse_tasks.extract_field as new_audit_qs { input :
      script_dir = "../ukbiobank",
      id = new_audit_id
    }
    File sc_new_audit_qs_ = new_audit_qs.data
  }

  call expanse_tasks.extract_field as new_audit_completion_date { input :
    script_dir = "../ukbiobank",
    id = 29202
  }

  call expanse_tasks.extract_field as month_of_birth { input:
    script_dir = "../ukbiobank",
    id = 52
  }

  call expanse_tasks.extract_field as year_of_birth { input:
    script_dir = "../ukbiobank",
    id = 34
  }

  output {
    #Array[File] sc_original_audit_qs = sc_original_audit_qs_
    File sc_original_audit_completion_date = original_audit_completion_date.data
    Array[File] sc_new_audit_qs = sc_new_audit_qs_
    File sc_new_audit_completion_date = new_audit_completion_date.data
    File sc_year_of_birth = year_of_birth.data
    File sc_month_of_birth = month_of_birth.data
  }
}
