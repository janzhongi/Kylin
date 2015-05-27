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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import java.io.File;
import java.io.IOException;

/**
 * Created by jiazhong on 2015/5/25.
 */
public class MonitorMetaManager {

    final static String TABLE_NAME = "kylin_monitor_metadata";
    final static String COLUMN_FAMILY = "t";
    final static String ROW_KEY_QUERY_READ_FILES = "query_log_files_already_read";
    final static String ROW_KEY_QUERY_READING_FILE = "query_log_file_reading";
    final static String ROW_KEY_QUERY_READING_FILE_LINE = "query_log_file_reading";


    final static String ROW_KEY_API_REQ_LOG_READ_FILES = "api_req_log_files_already_read";


    final static Logger logger = Logger.getLogger(MonitorMetaManager.class);

    static Configuration conf = null;

    static {
        conf = HBaseConfiguration.create();
    }

    public static void main(String[] args) throws Exception {

        // test_case_data/sandbox/ contains HDP 2.2 site xmls which is dev sandbox
        ConfigUtils.addClasspath(new File("../examples/test_case_data/sandbox").getAbsolutePath());
        conf = HBaseConfiguration.create();

//        MonitorMetaManager.getReadQueryLogFile();

//        MonitorMetaManager.markQueryFileAsRead("kylin_quyery.log-05-22");

        MonitorMetaManager.updateData(TABLE_NAME, ROW_KEY_QUERY_READ_FILES, COLUMN_FAMILY, "kylin_quyery.log-05-21");

//        MonitorMetaManager.getReadQueryLogFile();
    }


    /*
     * meta data initialize
     */
    public static void init() throws Exception {
        MonitorMetaManager.creatTable(TABLE_NAME, new String[]{COLUMN_FAMILY});
    }


    /*
     * mark query file as read after parsing
     */
    public static void markQueryFileAsRead(String filename) throws IOException {
        String read_query_log_file = MonitorMetaManager.getReadQueryLogFiles();
        if(read_query_log_file == null){
            read_query_log_file =  filename;
        }else{
            read_query_log_file = read_query_log_file.concat(",").concat(filename);
        }
        MonitorMetaManager.updateData(TABLE_NAME, ROW_KEY_QUERY_READ_FILES, COLUMN_FAMILY, read_query_log_file);
    }

    /*
     * mark reading file for tracking
     */
    public static void markQueryReadingFile(String query_reading_file) throws IOException {
        MonitorMetaManager.updateData(TABLE_NAME, ROW_KEY_QUERY_READING_FILE, COLUMN_FAMILY, query_reading_file);
    }

    /*
     * mark reading line for tracking
     */
    public static void markQueryReadingLine(String line_num) throws IOException {
        MonitorMetaManager.updateData(TABLE_NAME, ROW_KEY_QUERY_READING_FILE_LINE, COLUMN_FAMILY, line_num);
    }


    /*
     * get has been read file name list
     */
    public static String[] getReadQueryLogFileList() throws IOException {
        String fileList = MonitorMetaManager.getReadQueryLogFiles();
        return fileList.split(",");
    }

    /*
     * get has been read query log file
     */
    public static String getReadQueryLogFiles() throws IOException {
        return getListWithRowkey(TABLE_NAME, ROW_KEY_QUERY_READ_FILES);
    }


    /*
     * get has been read file
    */
    public static String getListWithRowkey(String table, String rowkey) throws IOException {
        Result result = getResultByRowKey(table, rowkey);
        String fileList = null;
        if (result.list() != null) {
            for (KeyValue kv : result.list()) {
                fileList = Bytes.toString(kv.getValue());
            }

        }
        fileList = fileList==null?"":fileList;
        return fileList;
    }


    /*
     * mark api req log file as read after parsing
     */
    public static void markApiReqLogFileAsRead(String filename) throws IOException {
        String read_api_req_log_files = MonitorMetaManager.getReadApiReqLogFiles();
        if (read_api_req_log_files == null) {
            read_api_req_log_files = filename;
        } else {
            read_api_req_log_files = read_api_req_log_files.concat(",").concat(filename);
        }
        MonitorMetaManager.updateData(TABLE_NAME, ROW_KEY_API_REQ_LOG_READ_FILES, COLUMN_FAMILY, read_api_req_log_files);
    }


    /*
     * get has been read log file name list
     */
    public static String[] getReadApiReqLogFileList() throws IOException {
        String fileList = MonitorMetaManager.getReadApiReqLogFiles();
        return fileList.split(",");
    }

    /*
    * get has been read api request log file
    */
    public static String getReadApiReqLogFiles() throws IOException {
        return getListWithRowkey(TABLE_NAME, ROW_KEY_API_REQ_LOG_READ_FILES);
    }


    /*
     * create table in hbase
     */
    public static void creatTable(String tableName, String[] family) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor desc = new HTableDescriptor(tableName);
        for (int i = 0; i < family.length; i++) {
            desc.addFamily(new HColumnDescriptor(family[i]));
        }
        if (admin.tableExists(tableName)) {
            logger.info("table Exists!");
        } else {
            admin.createTable(desc);
            logger.info("create table Success!");
        }
    }

    /*
     * update cell in hbase
     */
    public static void updateData(String tableName, String rowKey, String family, String value) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Put put = new Put(rowKey.getBytes());
        put.add(family.getBytes(), null, value.getBytes());
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("end insert data ......");
    }

    /*
     * get result by rowkey
     */
    public static Result getResultByRowKey(String tableName, String rowKey) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        return result;
    }


}
