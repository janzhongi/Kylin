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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * Created by jiazhong on 2015/4/28.
 */
public class ConfigUtils {

    final static Logger logger = Logger.getLogger(ConfigUtils.class);

    private static ConfigUtils ourInstance = new ConfigUtils();

    private Properties monitorConfig = new Properties();

    public static ConfigUtils getInstance() {
        return ourInstance;
    }

    private ConfigUtils() {
    }

    public static final String AIRLINE_FILES_BASE_DIR = "ext.airline.files.dir";
    public static final String KYLIN_METADATA_URL = "kylin.metadata.url";

    public static final String KYLIN_HOME = "KYLIN_HOME";
    public static final String KYLIN_CONF = "KYLIN_CONF";
    public static final String KYLIN_LOG_CONF_HOME = "KYLIN_LOG_CONF_HOME";
    public static final String CATALINA_HOME = "CATALINA_HOME";

    public static final String KYLIN_HDFS_WORKING_DIR = "kylin.hdfs.working.dir";

    public static final String KYLIN_MONITOR_CONF_PROP_FILE = "airline.properties";

    public static final String HIVE_JDBC_CON_USERNAME = "kylin.hive.jdbc.connection.username";
    public static final String HIVE_JDBC_CON_PASSWD = "kylin.hive.jdbc.connection.password";

    public static final String DEPLOY_ENV = "deploy.env";

    public static final String HIVE_JDBC_CON_URL = "kylin.hive.jdbc.connection.url";

    public void loadMonitorParam() throws IOException {
        Properties props = new Properties();
//        InputStream resourceStream = this.getAirlinePropertiesAsInputSteam();
        props.load(this.getClass().getClassLoader().getResourceAsStream("airline.properties"));
        this.monitorConfig = props;
    }

    public static InputStream getAirlinePropertiesAsInputSteam() {

        File propFile = getKylinMonitorProperties();
        if (propFile == null || !propFile.exists()) {
            logger.error("fail to locate airline.properties");
            throw new RuntimeException("fail to locate airline.properties");
        }
        try {
            return new FileInputStream(propFile);
        } catch (FileNotFoundException e) {
            logger.error("this should not happen");
            throw new RuntimeException(e);
        }

    }

    private static File getKylinMonitorProperties() {
//        String kylinConfHome = System.getProperty(KYLIN_CONF);
//        if (!StringUtils.isEmpty(kylinConfHome)) {
//            logger.info("load kylin.properties file from " + kylinConfHome + ". (from KYLIN_CONF System.property)");
//            return getKylinPropertiesFile(kylinConfHome);
//        }
//
//        logger.warn("KYLIN_CONF property was not set, will seek KYLIN_HOME env variable");
//
//        String kylinHome = getKylinHome();
//        if (StringUtils.isEmpty(kylinHome))
//            throw new RuntimeException("Didn't find KYLIN_CONF or KYLIN_HOME, please set one of them");
//
//        String path = kylinHome + File.separator + "conf";
//        logger.info("load kylin.properties file from " + kylinHome + ". (from KYLIN_HOME System.env)");
        return getKylinPropertiesFile(null);

    }

//    public static String getKylinHome() {
//        String kylinHome = System.getenv(KYLIN_HOME);
//        if (StringUtils.isEmpty(kylinHome)) {
//            logger.warn("KYLIN_HOME was not set");
//            return kylinHome;
//        }
//        logger.info("KYLIN_HOME is :" + kylinHome);
//        return kylinHome;
//    }

    private static File getKylinPropertiesFile(String path) {
        if (path == null) {
            return null;
        }
        return new File(path, KYLIN_MONITOR_CONF_PROP_FILE);
    }

    /*
     * get where to path log
     */
    public List<String> getAirlineFilesDir() {
        List<String> logDirList = new ArrayList<String>();

        String kylinExtLogBaseDir = getAirLineFilesBaseDir();
        if (!StringUtils.isEmpty(kylinExtLogBaseDir)) {
            String[] extPaths = kylinExtLogBaseDir.split(",");
            for (String path : extPaths) {
                if (!StringUtils.isEmpty(path)) {
                    logger.info("Use airline file dir=" + path);
                    logDirList.add(path.trim());
                }
            }
        }

        return logDirList;
    }

    public String getMetadataUrl() {
        return this.monitorConfig.getProperty(KYLIN_METADATA_URL);
    }

    public String getMetadataUrlPrefix() {
        String hbaseMetadataUrl = getMetadataUrl();
        String defaultPrefix = "kylin_metadata";
        int cut = hbaseMetadataUrl.indexOf('@');
        String tmp = cut < 0 ? defaultPrefix : hbaseMetadataUrl.substring(0, cut);
        return tmp;
    }

    public String getAirLineFilesBaseDir() {
        return this.monitorConfig.getProperty(AIRLINE_FILES_BASE_DIR);
    }

    public String getKylinHdfsWorkingDir() {
        String root = this.monitorConfig.getProperty(KYLIN_HDFS_WORKING_DIR);
        if (!root.endsWith("/")) {
            root += "/";
        }
        return root + getMetadataUrlPrefix();
    }

    public String getAirlineParseResultDir() {
        return this.getKylinHdfsWorkingDir() + "/airline/";
    }


    public String getRequestLogParseResultDir() {
        return this.getKylinHdfsWorkingDir() + "/performance/request/";
    }

    public String getHiveJdbcConUrl() {
        return this.monitorConfig.getProperty(HIVE_JDBC_CON_URL);
    }

    public String getLogParseResultMetaDir() {
        return this.getKylinHdfsWorkingDir() + "/performance/metadata/";
    }

    public String getDeployEnv() {
        return this.monitorConfig.getProperty(DEPLOY_ENV);
    }

    public String getHiveJdbcConUserName() {
        return this.monitorConfig.getProperty(HIVE_JDBC_CON_USERNAME);
    }

    public String getHiveJdbcConPasswd() {
        return this.monitorConfig.getProperty(HIVE_JDBC_CON_PASSWD);
    }

    public static void addClasspath(String path) throws Exception {
        File file = new File(path);

        if (file.exists()) {
            URLClassLoader urlClassLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
            Class<URLClassLoader> urlClass = URLClassLoader.class;
            Method method = urlClass.getDeclaredMethod("addURL", new Class[] { URL.class });
            method.setAccessible(true);
            method.invoke(urlClassLoader, new Object[] { file.toURI().toURL() });
        }
    }
}