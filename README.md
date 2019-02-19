Log Analyser
============


Implement map-reduce job for parsing web-server access log and counting all search requests.

Overview
=======================

Test project aimed to widen my professional specialization with the following technologies:
*Spring Boot
*Apache Hadoop (+ HBase)
*MapReduce


Functional requirements
=======================

* in every log string tries to detect search request to search engines (google, yandex) and extract search string
  output results to HBase: search keyword -> GoogleCount: N, YandexCount: M, where N and M are number of occurrences
  of keyword in search strings
* client-side application fetches some word and it’s neighbours (keywords which occur together in search queries)
  from HBase using REST and displays it as network: selected word - root, neighbours - children nodes;
  by child node click - fetches it’s neighbours from HBase and displays with clicked word as root


Current state
========================

0.1-SNAPSHOT
A mapreduce job being launched at the application startup. It reads plain text from the input directory, tries to find
search engine queries in the input files and counts word occurrences in them if any are found.
Results of the job are passed to hbase.

0.2
The job is refactored, tests are introduced.

Configuration
========================

Application awaits hdfs + hbase master node available at the specified in the application.yml address
("localhost" by default).

Set versions of your dependencies jars in the pom.xml accordingly with the versions of hdfs and hbase
(hdfs:2.7.3 hbase:1.2.4 by default).

Launch
========================

In the root of the project type int the terminal

1) mvn clean install
2) java -jar target/log-analyzer.jar