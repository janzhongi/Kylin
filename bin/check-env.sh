#!/bin/sh

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#by source

if [ -z "$KYLIN_HOME" ]
then
    echo 'please make sure KYLIN_HOME has been set'
    exit 1
else
    echo "KYLIN_HOME is set to ${KYLIN_HOME}"
fi

if [ -z "$(command -v hbase version)" ]
then
    echo "Please make sure the user has the privilege to run hbase shell"
    exit 1
fi

if [ -z "$(command -v hive --version)" ]
then
    echo "Please make sure the user has the privilege to run hive shell"
    exit 1
fi

if [ -z "$(command -v hadoop version)" ]
then
    echo "Please make sure the user has the privilege to run hadoop shell"
    exit 1
fi
