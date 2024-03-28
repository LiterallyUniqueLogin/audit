version 1.0

task rl {

  command <<<
    echo "
    foo
    bar
    baz
    booz"
  >>>

  output {
    Array[String] lines = read_lines(stdout())
  }

  runtime {
    memory: "2GB"
    dx_timeout: "30m"
  }
}

workflow w {
  call rl

  output {
    Array[String] lines = rl.lines
  }
}
