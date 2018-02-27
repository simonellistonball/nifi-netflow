<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Netflow Parser for NiFi

This is a WIP (i.e. it does not work)

The bundle will contain NiFi processors to consume and Parse Netflow 9 including options and templates

## Known Issues:

If you reeceive a packet which only consists of templates, which is reaonsably common, it will lead to the flow file being sent to parse.failure, since no actual flow records are emitted. Fixing this requires some upstream changes in the record reader api to allow flow files with no records to be ignored. 

## Building 

I build against NiFi 1.5.0, but 1.4.0 should work. This is my build command (note I keep a copy of NiFi lying around in home, and have added custom to the NiFi library paths)

```
mvn package && \
cp nifi-netflow-nar/target/nifi-netflow-nar-0.0.1.nar \
~/nifi-1.5.0-SNAPSHOT/custom/
```

copy the resulting nar from nifi-netflow-nar/target to your nifi custom library directory and restart nifi.
