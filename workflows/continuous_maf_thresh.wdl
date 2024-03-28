version 1.0

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
    head -2 ~{file} > out.tab
  >>>

  output {
    File out = 'out.tab'
  }

  runtime {
    dx_timeout: "2h"
    memory: "2GB"
  }
}

task subset_results {
  input {
    File plink_results
  }

  output {
    File subsetted_results = 'subsetted_results.tab'
  }

  command <<<
    python -c '
    import polars as pl
    pl.scan_csv(
      "~{plink_results}",
      separator="\t",
      null_values="NA"
    ).filter(
      pl.min_horizontal(pl.col("A1_FREQ"), 1-pl.col("A1_FREQ")) >= 0.005
    ).collect().write_csv(
      "subsetted_results.tab", separator="\t"
    )
    '
  >>>

  runtime {
    dx_timeout: "30m"
    memory: "30GB"
    docker: "quay.io/thedevilinthedetails/work/python_data:v1.0"
  }
}

workflow continuous_maf_thresh {
  Array[String] target_npy_names = [
    "combined_p",
    "combined_p_log10",
    "combined_q4",
    "combined_q5",
    "combined_q6",
    "combined_q7",
    "combined_q8",
    "combined_q9",
    "combined_q10",
  ]

  call expanse_files.files

  scatter (idx in range(length(target_npy_names))) {
    call subset_results { input :
      plink_results = "data/gwas_results/~{target_npy_names[idx]}/white_brits_snp_gwas.tab"
    }

    call gwas_tasks.qq_plot { input :
      script_dir = "../ukbiobank",
      results_tab = subset_results.subsetted_results,
      p_val_col = 'P',
      phenotype_name = "audit_~{target_npy_names[idx]}",
      variant_type = 'SNP',
      out_name = 'audit_~{target_npy_names[idx]}_maf_more_0.05_snp_qq_plot',
      null_values='NA'
    }

		call gwas_tasks.generate_peaks as overview_manhattan_peaks { input :
      script_dir = "../ukbiobank",
      str_assoc_results = "data/gwas_results/~{target_npy_names[idx]}/white_brits_str_gwas.tab",
      snp_assoc_results = subset_results.subsetted_results,
      phenotype = "audit_~{target_npy_names[idx]}",
			spacing = "20000000",
			thresh = "5e-8"
		}

    call gwas_tasks.overview_manhattan { input :
      script_dir = "../ukbiobank",
      phenotype_name = "audit_~{target_npy_names[idx]}",
      chr_lens = files.chr_lens,
      str_gwas_results = "data/gwas_results/~{target_npy_names[idx]}/white_brits_str_gwas.tab",
      snp_gwas_results = subset_results.subsetted_results,
      ext = "png",
      prefix = 'audit_~{target_npy_names[idx]}_maf_more_0.05_',
      peaks = overview_manhattan_peaks.peaks
    }

    call move_files_to_dir { input :
      files = [
        qq_plot.plot,
        overview_manhattan.plot
      ],
      dir_name = "~{target_npy_names[idx]}"
    }
    Array[File] moved_files = move_files_to_dir.out
  }

  output {
    Array[Array[File]] out = moved_files
  }
}
