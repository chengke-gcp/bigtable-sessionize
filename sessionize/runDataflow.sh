mvn compile exec:java -Dexec.mainClass=Sessionize -Dexec.args="--project=bigtable-sessionize \
      --tempLocation=gs://bigtable-sessionize/dataflow \
      --zone=us-central1-a \
      --runner=dataflow \
      --autoscalingAlgorithm=THROUGHPUT_BASED \
      --maxNumWorkers=10"
