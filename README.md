logback-flume
=============

A logback appender to flume.

A "port" of the code in log4j 2.0 alpha flume appender, adapted to logback.

Most features of the log4j appender have been kept.

There are 2 available modes : 

- "avro" : the appender writes to a remote avro source directly

Pros : only a few dependecies 
Cons : loses messages if the connection to the avro source is lost

- "agent" : the appender behaves like a flume agent

Pros : cannot lose log messages
Cons : a lot of dependecies


