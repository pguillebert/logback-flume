(defproject youmag/logback-flume "1.0.0"
  :description "flume appender for logback"
  :dependencies [
                 [junit/junit "4.10"]
                 [ch.qos.logback/logback-classic "1.0.9"]
                 [org.apache.flume/flume-ng-sdk "1.3.1"
                  :exclusions [org.slf4j/slf4j-log4j12
                               log4j/log4j]]
                 [org.apache.flume/flume-ng-core "1.3.1"
                  :exclusions [org.slf4j/slf4j-log4j12
                               log4j/log4j]]
                 [org.apache.flume/flume-ng-node "1.3.1"
                  :exclusions [org.slf4j/slf4j-log4j12
                               log4j/log4j]]
                 [org.apache.flume.flume-ng-channels/flume-file-channel "1.3.1"
                  :exclusions [org.slf4j/slf4j-log4j12
                               log4j/log4j]]

                 [org.apache.hadoop/hadoop-core "1.0.3"]
                 ]
  :java-source-paths ["src/main/java" "src/test/java"]
  :javac-options ["-Xlint:unchecked"]
  :aot :all)
