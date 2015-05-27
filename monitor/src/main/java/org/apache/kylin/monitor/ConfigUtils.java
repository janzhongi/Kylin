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


package org.apache.kylin.monitor;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import java.io.*;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;

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


    public static final String KYLIN_HOME = "KYLIN_HOME";
    public static final String KYLIN_CONF = "KYLIN_CONF";
    public static final String KYLIN_LOG_CONF_HOME = "KYLIN_LOG_CONF_HOME";
    public static final String CATALINA_HOME = "CATALINA_HOME";
    public static final String KYLIN_MONITOR_CONF_PROP_FILE = "kylin.properties";
    public static final String LOG_PARSE_RESULT_DIR = "log.parse.result.dir";
    public static final String QUERY_PARSE_RESULT_FILE_NAME = "query.log.parse.result.filename";
    public static final String QUERY_LOG_FILE_PATTERN = "query.log.file.pattern";

    public static final String REQUEST_PARSE_RESULT_FILE_NAME = "request.log.parse.result.filename";
    public static final String REQUEST_LOG_FILE_PATTERN = "request.log.file.pattern";


    public void loadMonitorParam() throws IOException {
        Properties props = new Properties();
        InputStream resourceStream = this.getKylinPropertiesAsInputSteam();
        props.load(resourceStream);
        this.monitorConfig = props;
    }


    public static InputStream getKylinPropertiesAsInputSteam() {
        File propFile = getKylinMonitorProperties();
        if (propFile == null || !propFile.exists()) {
            logger.error("fail to locate kylin.properties");
            throw new RuntimeException("fail to locate kylin.properties");
        }
        try {
            return new FileInputStream(propFile);
        } catch (FileNotFoundException e) {
            logger.error("this should not happen");
            throw new RuntimeException(e);
        }

    }


    private static File getKylinMonitorProperties() {
        String kylinConfHome = System.getProperty(KYLIN_CONF);
        if (!StringUtils.isEmpty(kylinConfHome)) {
            logger.info("Use KYLIN_CONF=" + kylinConfHome);
            return getKylinPropertiesFile(kylinConfHome);
        }

        logger.warn("KYLIN_CONF property was not set, will seek KYLIN_HOME env variable");

        String kylinHome = getKylinHome();
        if (StringUtils.isEmpty(kylinHome))
            throw new RuntimeException("Didn't find KYLIN_CONF or KYLIN_HOME, please set one of them");

        String path = kylinHome + File.separator + "conf";
        return getKylinPropertiesFile(path);

    }

    public static String getKylinHome() {
        String kylinHome = System.getenv(KYLIN_HOME);
        if (StringUtils.isEmpty(kylinHome)) {
            logger.warn("KYLIN_HOME was not set");
            return kylinHome;
        }
        logger.info("KYLIN_HOME is :"+kylinHome);
        return kylinHome;
    }

    private static File getKylinPropertiesFile(String path) {
        if (path == null) {
            return null;
        }
        return new File(path, KYLIN_MONITOR_CONF_PROP_FILE);
    }


    public String getLogBaseDir() {

        String kylinLogConfHome = System.getProperty(KYLIN_LOG_CONF_HOME);
        if (!StringUtils.isEmpty(kylinLogConfHome)) {
            logger.info("Use KYLIN_LOG_CONF_HOME=" + KYLIN_LOG_CONF_HOME);
            return kylinLogConfHome;
        }

        String kylinHome = getKylinHome();
        if (!StringUtils.isEmpty(kylinHome))
            throw new RuntimeException("Didn't find KYLIN_CONF or KYLIN_HOME, please set one of them");
        String path = kylinHome + File.separator + "tomcat" + File.separator + "logs";
        return path;
    }

    public String getLogParseResultDir() {
        return this.monitorConfig.getProperty(LOG_PARSE_RESULT_DIR);
    }

    public String getQueryParseResultFileName() {
        return this.monitorConfig.getProperty(QUERY_PARSE_RESULT_FILE_NAME);
    }

    public String getQueryLogFilePattern() {
        return this.monitorConfig.getProperty(QUERY_LOG_FILE_PATTERN);
    }


    //API Request
    public String getRequestParseResultFileName() {
        return this.monitorConfig.getProperty(REQUEST_PARSE_RESULT_FILE_NAME);
    }

    public String getRequestLogFilePattern() {
        return this.monitorConfig.getProperty(REQUEST_LOG_FILE_PATTERN);
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
