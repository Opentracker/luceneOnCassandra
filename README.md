# LuceneOnCassandra
=================

The goal of this project is to store the Apache lucene index in Apache Cassandra.
This means that Cassandra will serve as the medium for indexing and querying without changes to other Lucene indexing and querying APIs. In other words, Cassandra will only be used as the datastore for the index files, and the rest of the logic will go through lucene’s indexing procedures.

*Apache Cassandra 1.0.8 and Apache Lucene 4.8.1 are included in this project.*


## Getting Started
Change the following configuration files in docs/apache-cassandra-1.0.8:

    docs/apache-cassandra-1.0.8/conf/cassandra.yaml
    docs/apache-cassandra-1.0.8/conf/log4j-server.properties


We have included multiple tests cases and examples to understand how it works.
You need Eclipse and JUnit to run these.

To run these examples and tests, the first step is to start the Cassandra 'instance' locally.

1. cd to docs/apache-cassandra-1.0.8/bin.
2. start Cassandra with: 
   $ cassandra -f
3. run pom.xml or in Eclipse using JUnit.

## License

<pre>
This software is licensed under the Apache 2 license, quoted below.

Copyright 2003-2014 Opentracker <http://www.opentracker.net>

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
</pre>,lljl
