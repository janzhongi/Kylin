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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVWriter;

/**
 * @author jiazhong
 */
public class AirLineParser {

    final static Logger logger = Logger.getLogger(AirLineParser.class);
    final static Charset ENCODING = StandardCharsets.UTF_8;
    final static String AIRLINE_FILE_PATTERN = "On_Time_On_Time_Performance(.*).*$";
    final static String AIRLINE_PARSE_RESULT_FILENAME = "airline/airline.csv";
    final static String ORIGIN_PARSE_RESULT_FILENAME = "origin/origin.csv";
    final static String DEST_PARSE_RESULT_FILENAME = "dest/dest.csv";
    static String AIRLINE_PARSE_RESULT_PATH = null;
    static String ORIGIN_PARSE_RESULT_PATH = null;
    static String DEST_PARSE_RESULT_PATH = null;

    final static String[] AIRLINE_HEADER = { "FLIGHT_ID", "FLIGHT_DATE", "FLIGHT_NUM","DEP_TIME","DEP_DELAY","ARRIVAL_TIME","ARRIVAL_DELAY","UNIQUE_CARRIER","ORIGIN_AIRPORT_ID","DEST_AIRPORT_ID"};
    final static String[] ORIGIN_HEADER = { "ORIGIN_AIRPORT_ID","ORIGIN","ORIGIN_CITY","ORIGIN_STATE"};
    final static String[] DEST_HEADER = { "DEST_AIRPORT_ID","DEST","DEST_CITY","DEST_STATE"};

    List<File> logFiles = new ArrayList<File>();

    HashSet<List<String>> uniqOrigin = new HashSet<>();
    HashSet<List<String>> uniqDest = new HashSet<>();

    private ConfigUtils monitorConfig;

    public AirLineParser() {
        monitorConfig = ConfigUtils.getInstance();
        try {
            monitorConfig.loadMonitorParam();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
     * will parse kylin query log files ,and append result to csv on hdfs
     * files read will be marked,will not read again
     */

    public void start() throws IOException, ParseException {
        AirLineParser.AIRLINE_PARSE_RESULT_PATH = ConfigUtils.getInstance().getAirlineParseResultDir() + AIRLINE_PARSE_RESULT_FILENAME;
        AirLineParser.ORIGIN_PARSE_RESULT_PATH = ConfigUtils.getInstance().getAirlineParseResultDir() + ORIGIN_PARSE_RESULT_FILENAME;
        AirLineParser.DEST_PARSE_RESULT_PATH = ConfigUtils.getInstance().getAirlineParseResultDir() + DEST_PARSE_RESULT_FILENAME;
        this.parseInit();

        //get query file has been read
        String[] hasReadFiles = MonitorMetaManager.getAirlineFileList();
//        String[] hasReadFiles = new String[]{};

        //get all airLine files
        List<File> files = this.getAirlineFiles();

        for (File file : files) {
            if (!Arrays.asList(hasReadFiles).contains(file.getName())) {
                this.parseAirlineFile(file.getPath());
//                MonitorMetaManager.markFileAsRead(file.getName());
            }
        }

        this.writeOrigin();
        this.writeDest();
    }

    public void parseInit() throws IOException {
        logger.info("parse airline initializing...");
        FileSystem fs = null;
        try {
            Configuration conf = new Configuration();
            fs = FileSystem.get(conf);
            org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(AirLineParser.AIRLINE_PARSE_RESULT_PATH);
            if (!fs.exists(path)) {
                fs.create(path);
                fs.close(); //need to close before get FileSystem again
                this.writeResultToHdfs(AirLineParser.AIRLINE_PARSE_RESULT_PATH, AirLineParser.AIRLINE_HEADER);
            }

            fs = FileSystem.get(conf);
            org.apache.hadoop.fs.Path originPath = new org.apache.hadoop.fs.Path(AirLineParser.ORIGIN_PARSE_RESULT_PATH);
            if (!fs.exists(originPath)) {
                fs.create(originPath);
                fs.close(); //need to close before get FileSystem again
                this.writeResultToHdfs(AirLineParser.ORIGIN_PARSE_RESULT_PATH, AirLineParser.ORIGIN_HEADER);
            }

            fs = FileSystem.get(conf);
            org.apache.hadoop.fs.Path destPath = new org.apache.hadoop.fs.Path(AirLineParser.DEST_PARSE_RESULT_PATH);
            if (!fs.exists(destPath)) {
                fs.create(destPath);
                fs.close(); //need to close before get FileSystem again
                this.writeResultToHdfs(AirLineParser.DEST_PARSE_RESULT_PATH, AirLineParser.DEST_HEADER);
            }

        } catch (IOException e) {
            if(fs != null) {
                fs.close();
            }
            logger.info("Failed to init:", e);
        }
    }

    //parse query log and convert to csv file to hdfs
    public void parseAirlineFile(String filePath) throws ParseException, IOException {

        logger.info("Start parsing file " + filePath + " !");

        //        writer config init
//        FileSystem fs = this.getHdfsFileSystem();
//        org.apache.hadoop.fs.Path airlinePath = new org.apache.hadoop.fs.Path(AirLineParser.AIRLINE_PARSE_RESULT_PATH);
//        org.apache.hadoop.fs.Path destPath = new org.apache.hadoop.fs.Path(AirLineParser.DEST_PARSE_RESULT_PATH);
//
//        OutputStreamWriter airLineWriter = new OutputStreamWriter(fs.append(airlinePath));
//        CSVWriter lineHdfsWriter = new CSVWriter(airLineWriter, '|', CSVWriter.NO_QUOTE_CHARACTER);




        Path path = Paths.get(filePath);
        try {
            BufferedReader reader = Files.newBufferedReader(path, ENCODING);
            reader.readLine();
            String line = null;
            while ((line = reader.readLine()) != null) {

                String[] record = line.split(",");
                String id = UUID.randomUUID().toString();
                String date = record[0]+"-"+record[2]+"-"+record[3];
                String[] airlineRecord = new String[]{id,date,record[10].replace("\"",""),record[32].replace("\"",""),record[33].replace("\"",""),record[43].replace("\"",""),record[44],record[6].replace("\"", ""),record[11].replace("\"",""),record[21].replace("\"","")};
                String[] originRecord = new String[]{record[11].replace("\"",""),record[14].replace("\"",""),record[15].replace("\"",""),record[16].replace("\"","")};
                String[] destRecord = new String[]{record[21].replace("\"", ""),record[24].replace("\"",""),record[25].replace("\"",""),record[26].replace("\"","")};
                uniqOrigin.add(Arrays.asList(originRecord));
                uniqDest.add(Arrays.asList(destRecord));
//                lineHdfsWriter.writeNext(airlineRecord);
            }
        } catch (IOException ex) {
            logger.info("Failed to write to hdfs:", ex);
        } finally {
//            if(lineHdfsWriter != null) {
//                lineHdfsWriter.close();
//            }
//             if(airLineWriter != null) {
//                lineHdfsWriter.close();
//            }
//            if(fs != null) {
//                fs.close();
//            }
        }

        logger.info("Finish parsing file " + filePath + " !");

    }

    public void writeOrigin() throws IOException {
        org.apache.hadoop.fs.Path originPath = new org.apache.hadoop.fs.Path(AirLineParser.ORIGIN_PARSE_RESULT_PATH);
        FileSystem fs = this.getHdfsFileSystem();
        OutputStreamWriter originWriter = new OutputStreamWriter(fs.append(originPath));
        CSVWriter  originHdfsWriter = new CSVWriter(originWriter, '|', CSVWriter.NO_QUOTE_CHARACTER);

        List<String[]> origin_arr = new ArrayList<>();
        for(List<String> origin:uniqOrigin){
            origin_arr.add(origin.toArray(new String[origin.size()]));
        }

        originHdfsWriter.writeAll(origin_arr);
        if(originWriter!=null){
            originWriter.close();
        }
        if(originHdfsWriter!=null){
            originHdfsWriter.close();
        }

    }

    public void writeDest() throws IOException {
        org.apache.hadoop.fs.Path destPath = new org.apache.hadoop.fs.Path(AirLineParser.DEST_PARSE_RESULT_PATH);
        FileSystem fs = this.getHdfsFileSystem();
        OutputStreamWriter destWriter = new OutputStreamWriter(fs.append(destPath));
        CSVWriter  destHdfsWriter = new CSVWriter(destWriter, '|', CSVWriter.NO_QUOTE_CHARACTER);

        List<String[]> dest_arr = new ArrayList<>();
        for(List<String> dest:uniqDest){
            dest_arr.add(dest.toArray(new String[dest.size()]));
        }
        destHdfsWriter.writeAll(dest_arr);

        if(destWriter!=null){
            destWriter.close();
        }
        if(destHdfsWriter!=null){
            destHdfsWriter.close();
        }
    }


    /*
     * write parse result to hdfs
     */
    public void writeResultToHdfs(String dPath, String[] record) throws IOException {
        OutputStreamWriter writer = null;
        CSVWriter cwriter = null;
        FileSystem fs = null;
        try {
            fs = this.getHdfsFileSystem();
            org.apache.hadoop.fs.Path resultStorePath = new org.apache.hadoop.fs.Path(dPath);
            writer = new OutputStreamWriter(fs.append(resultStorePath));
            cwriter = new CSVWriter(writer, '|', CSVWriter.NO_QUOTE_CHARACTER);

            cwriter.writeNext(record);

        } catch (IOException e) {
            logger.info("Exception", e);
        } finally {
            if(writer != null) {
                writer.close();
            }
            if(cwriter != null) {
                cwriter.close();
            }
            if(fs != null) {
                fs.close();
            }
        }
    }

    /*
     * get all query log files
     */
    public List<File> getAirlineFiles() {

        List<String> airline_dir_list = monitorConfig.getAirlineFilesDir();


        for (String path : airline_dir_list) {
            logger.info("fetching airline file from path:" + path);
            File airline_file_dir = new File(path);
            listFileRecursive(airline_file_dir);
//            File[] airline_files = airline_file_dir.listFiles();
//            if (airline_files == null) {
//                logger.warn("no airline file found under path" + path);
//                continue;
//            }
//            Collections.addAll(logFiles, airline_files);
        }
        return logFiles;
    }

    public void listFileRecursive(File file){
        FileFilter filter = new RegexFileFilter(AIRLINE_FILE_PATTERN);
        File[] files = file.listFiles(filter);
        for(File _file:files){
            if(_file.isDirectory()){
                listFileRecursive(_file);
            }else{
                logFiles.add(_file);
            }
        }
    }
    /*
     * get hdfs fileSystem
     */
    public FileSystem getHdfsFileSystem() throws IOException {
        Configuration conf = new Configuration();
        //        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            if(fs != null) {
                fs.close();
            }
            logger.info("Failed to get hdfs FileSystem", e);
        }
        return fs;
    }

}