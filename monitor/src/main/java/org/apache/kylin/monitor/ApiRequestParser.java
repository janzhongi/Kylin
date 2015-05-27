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

import au.com.bytecode.opencsv.CSVWriter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author jiazhong
 */
public class ApiRequestParser {

    final static Logger logger = Logger.getLogger(ApiRequestParser.class);

    final static Charset ENCODING = StandardCharsets.UTF_8;

    static String REQUEST_PARSE_RESULT_PATH = null;

    final static String[] KYLIN_REQUEST_CSV_HEADER = {"REQUESTER", "REQ_TIME", "URI", "METHOD", "QUERY_STRING", "PAYLOAD","RESP_STATUS", "TARGET", "ACTION"};

    private ConfigUtils monitorConfig;

    public ApiRequestParser() {
        monitorConfig = ConfigUtils.getInstance();
        try {
            monitorConfig.loadMonitorParam();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start() throws IOException, ParseException {
        ApiRequestParser.REQUEST_PARSE_RESULT_PATH = ConfigUtils.getInstance().getLogParseResultDir() + ConfigUtils.getInstance().getRequestParseResultFileName();
        this.parseRequestInit();

        //get api req log files have been read
        String[] hasReadFiles = MonitorMetaManager.getReadApiReqLogFileList();

        File[] files = this.getRequestLogFiles();
        for (File file : files) {
            if(!Arrays.asList(hasReadFiles).contains(file.getName())) {
                this.parseRequestLog(file.getPath(), ApiRequestParser.REQUEST_PARSE_RESULT_PATH);
                MonitorMetaManager.markApiReqLogFileAsRead(file.getName());
            }
        }
    }

    public void parseRequestInit() throws IOException {
        FileSystem fs = null;
        try {
            Configuration conf = new Configuration();
            conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
            fs = FileSystem.get(conf);
            org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(ApiRequestParser.REQUEST_PARSE_RESULT_PATH);
            if (!fs.exists(path)) {
                fs.create(path);

                //need to close before get FileSystem again
                fs.close();
                this.writeResultToHdfs(ApiRequestParser.REQUEST_PARSE_RESULT_PATH, ApiRequestParser.KYLIN_REQUEST_CSV_HEADER);
            }
        } catch (IOException e) {
            fs.close();
            logger.info("Failed to init:", e);
        }
    }

    //parse query log and convert to csv file to hdfs
    public void parseRequestLog(String filePath, String dPath) throws ParseException, IOException {

        logger.info("Start parsing kylin api request file " + filePath + " !");

//        writer config init
        FileSystem fs = this.getHdfsFileSystem();
        org.apache.hadoop.fs.Path resultStorePath = new org.apache.hadoop.fs.Path(dPath);
        OutputStreamWriter writer = new OutputStreamWriter(fs.append(resultStorePath));
        CSVWriter cwriter = new CSVWriter(writer);

        Pattern p_available = Pattern.compile("/kylin/api/(cubes|user)+.*");
        Pattern p_request = Pattern.compile("^.*\\[.*KylinApiFilter.logRequest.*\\].*REQUEST:.*REQUESTER=(.*);REQ_TIME=(.*);URI=(.*);METHOD=(.*);QUERY_STRING=(.*);PAYLOAD=(.*);RESP_STATUS=(.*);$");
        Pattern p_uri = Pattern.compile("/kylin/api/(\\w+)(/.*/)*(.*)$");
        Matcher m_available = p_available.matcher("");
        Matcher m_request = p_request.matcher("");
        Matcher m_uri = p_uri.matcher("");

        Path path = Paths.get(filePath);
        try (
                BufferedReader reader = Files.newBufferedReader(path, ENCODING);
        ) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                //reset the input
                m_available.reset(line);
                m_request.reset(line);

                //filter unnecessary info
                if(m_available.find()) {

                    //filter GET info
                    if (m_request.find()&&!m_request.group(4).equals("GET")) {

                        List<String> groups = new ArrayList<String>();
                        for (int i = 1; i <= m_request.groupCount(); i++) {
                            groups.add(m_request.group(i));
                        }

                        String uri = m_request.group(3);
                        m_uri.reset(uri);
                        if (m_uri.find()) {

                            //add target
                            groups.add(m_uri.group(1));

                            //add action
                            if(m_uri.group(1).equals("cubes")){
                                switch (m_request.group(4)){
                                    case "DELETE":
                                        groups.add("drop");
                                        break;
                                    case "POST":
                                        groups.add("save");
                                        break;
                                    default:
                                        //add parse action
                                        groups.add(m_uri.group(3));
                                        break;
                                }
                            }

                        }
                        String[] recordArray = groups.toArray(new String[groups.size()]);
                        //write to hdfs
                        cwriter.writeNext(recordArray);
                    }
                }


            }
        } catch (IOException ex) {
            logger.info("Failed to write to hdfs:", ex);
        } finally {
            writer.close();
            cwriter.close();
            fs.close();
        }

        logger.info("Finish parsing file " + filePath + " !");

    }

    public void writeResultToHdfs(String dPath, String[] record) throws IOException {
        OutputStreamWriter writer = null;
        CSVWriter cwriter = null;
        FileSystem fs = null;
        try {
            fs = this.getHdfsFileSystem();
            org.apache.hadoop.fs.Path resultStorePath = new org.apache.hadoop.fs.Path(dPath);
            writer = new OutputStreamWriter(fs.append(resultStorePath));
            cwriter = new CSVWriter(writer);

            cwriter.writeNext(record);

        } catch (IOException e) {
            logger.info("Exception", e);
        } finally {
            writer.close();
            cwriter.close();
            fs.close();
        }
    }

    public File[] getRequestLogFiles() {

        String request_log_file_pattern = monitorConfig.getRequestLogFilePattern();

        String request_log_dir_path = monitorConfig.getLogBaseDir();

        File request_log_dir = new File(request_log_dir_path);

        FileFilter filter = new RegexFileFilter(request_log_file_pattern);

        File[] request_log_files = request_log_dir.listFiles(filter);

        return request_log_files;
    }

    public FileSystem getHdfsFileSystem() throws IOException {
        Configuration conf = new Configuration();
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            fs.close();
            logger.info("Failed to get hdfs FileSystem", e);
        }
        return fs;
    }

}
