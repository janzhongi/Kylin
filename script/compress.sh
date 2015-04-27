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

# pack webapp into kylin.war so that we have a all-in-one war
dir=$(dirname ${0})
cd ${dir}/..

if [ -z "$version" ]
then
    echo 'version not set'
    version=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -v '\['`
    echo "${version}"
fi

#package tar.gz
echo 'package tar.gz'
rm -rf kylin-${version}
mkdir kylin-${version}
cp -r lib bin conf tomcat examples/sample_cube kylin-${version}
find kylin-${version} -type d -exec chmod 755 {} \;
find kylin-${version} -type f -exec chmod 644 {} \;
find kylin-${version} -type f -name "*.sh" -exec chmod 755 {} \;
tar -cvzf kylin-${version}.tar.gz kylin-${version}
rm -rf kylin-${version}

echo "Package ready kylin-${version}.tar.gz"
