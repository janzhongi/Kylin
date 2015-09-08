/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.airline;

import org.apache.log4j.Logger;

/**
 * Created by jiazhong on 2015/5/7
 */
public class DebugAirlineParseClient {

    static {
        //set catalina.home temp
        System.setProperty(ConfigUtils.CATALINA_HOME, "../server/");
    }

    final static Logger logger = Logger.getLogger(DebugAirlineParseClient.class);

    public static void main(String[] args) throws Exception {

        AirLineParser airLineParser = new AirLineParser();
        HiveJdbcClient jdbcClient = new HiveJdbcClient();

        try {
            MonitorMetaManager.init();
            airLineParser.start();
//            jdbcClient.start();
        } catch (Exception e) {
            logger.info("Exception ", e);
        }
    }

}