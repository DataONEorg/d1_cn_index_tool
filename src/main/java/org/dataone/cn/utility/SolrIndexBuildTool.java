/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For 
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 * 
 * $Id$
 */

package org.dataone.cn.utility;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.cn.index.generator.IndexTaskGenerator;
import org.dataone.cn.index.processor.IndexTaskProcessor;
import org.dataone.cn.index.task.IndexTask;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v2.SystemMetadata;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.IMap;

public class SolrIndexBuildTool {
    private static Logger logger = Logger.getLogger(SolrIndexBuildTool.class.getName());

    private static final String HZ_SYSTEM_METADATA = Settings.getConfiguration().getString(
            "dataone.hazelcast.systemMetadata");

    private static final String HZ_OBJECT_PATH = Settings.getConfiguration().getString(
            "dataone.hazelcast.objectPath");

    private static final String HZ_IDENTIFIERS = Settings.getConfiguration().getString(
            "dataone.hazelcast.identifiers");

    private static final String DEFAULT_INDEX_APPLICATION_CONTEXT = "index-tool-context.xml";
    private static final String NEXT_INDEX_APPLICATION_CONTEXT = "index-tool-next-context.xml";

    /** whether to add documents in batch - avoiding multiple solr commands */
    private static boolean BATCH_UPDATE = Settings.getConfiguration().getBoolean("dataone.indexing.tool.batchUpdate",  false);
    
    /** the number of documents to process as a time */
    private static int BATCH_UPDATE_SIZE = Settings.getConfiguration().getInt("dataone.indexing.batchUpdateSize",  1000);
    
    /** the number of index task will be generate on one cycle */
    private static int INDEX_TASK_ONE_CYCLE_SIZE = Settings.getConfiguration().getInt("dataone.indexing.tool.indexTaskOneCycleSize",  1000);
        
    private HazelcastClient hzClient;
    private IMap<Identifier, SystemMetadata> systemMetadata;
    private IMap<Identifier, String> objectPaths;
    private Set<Identifier> pids;
    private ApplicationContext context;

    private IndexTaskGenerator generator;
    private IndexTaskProcessor processor;

    private boolean buildNextIndex = false;

    public SolrIndexBuildTool() {
    }

    public static void main(String[] args) {
        DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
        Date dateParameter = null;
        String dateString = null;
        boolean help = false;
        boolean fullRefresh = false;
        boolean migrate = false;
        String pidFile = null;
        int totalToProcess = 0;
        int startIndex = 0;
        int options = 0;
        for (String arg : args) {
            if (StringUtils.startsWith(arg, "-d")) {
                dateString = StringUtils.substringAfter(arg, "-d");
                dateString = StringUtils.trim(dateString);
                try {
                    dateParameter = dateFormat.parse(dateString);
                    options++;
                } catch (ParseException e) {
                    System.out.println("Unable to parse provided date string: " + dateString);
                }
            } else if (StringUtils.startsWith(arg, "-help")) {
                help = true;
            } else if (StringUtils.startsWith(arg, "-a")) {
                fullRefresh = true;
                options++;
            } else if (StringUtils.startsWith(arg, "-migrate")) {
                migrate = true;
            } else if (StringUtils.startsWith(arg, "-pidFile")) {
                pidFile = StringUtils.trim(StringUtils.substringAfter(arg, "-pidFile"));
                options++;
            } else if (StringUtils.startsWith(arg, "-startAt")) {
                options++;
                String startAt = StringUtils.trim(StringUtils.substringAfter(arg, "-startAt"));
                startIndex = Integer.valueOf(startAt).intValue();
            } else if (StringUtils.startsWith(arg, "-c")) {
                String countStr = StringUtils.trim(StringUtils.substringAfter(arg, "-c"));
                totalToProcess = Integer.valueOf(countStr).intValue();
                options++;
            }
        }

        if (help
                || (fullRefresh == false && dateParameter == null && pidFile == null && startIndex == 0)) {
            showHelp();
            return;
        }
        if (options > 1) {
            System.out
                    .println("Only one option amoung -a, -d, -c, -pidFile, -startAt may be used at a time.");
            showHelp();
            return;
        } else if (options == 0) {
            System.out
                    .println("At least one option amoung -a, -d, -c, -pidFile, -startAt must be specified.");
        }

        if (fullRefresh) {
            System.out.println("Performing full build/refresh of solr index.");
        } else if (dateParameter != null) {
            System.out.println("Performing (re)build from date: "
                    + dateFormat.format(dateParameter) + ".");
        } else if (pidFile != null) {
            System.out.println("Performing refresh/index for pids found in file: " + pidFile);
        }

        if (migrate) {
            System.out
                    .println("Performing refresh/build against the next version of search index.");
        } else {
            System.out.println("Performing refresh/build against live search index.");
        }

        System.out.println("Starting solr index refresh.");

        SolrIndexBuildTool indexTool = new SolrIndexBuildTool();
        indexTool.setBuildNextIndex(migrate);
        try {
            refreshSolrIndex(indexTool, dateParameter, pidFile, totalToProcess, startIndex);
        } catch (Exception e) {
            System.out.println("Solr index refresh failed: " + e.getMessage());
            e.printStackTrace(System.out);
        }

        System.out.println("Exiting solr index refresh tool.");
    }

    private static void refreshSolrIndex(SolrIndexBuildTool indexTool, Date dateParameter,
            String pidFilePath, int totalToProcess, int startIndex) {
        indexTool.configureContext();
        indexTool.configureHazelcast();

        System.out.println("Starting re-indexing... (" + (new Date()) + ")");
        
        if (pidFilePath == null) {
            indexTool.generateIndexTasksAndProcess(dateParameter, totalToProcess, startIndex);
        } else {
            indexTool.updateIndexForPids(pidFilePath);
        }
        try {
            Queue<Future> futures = indexTool.getIndexTaskProcessor().getFutureQueue();
            for(Future future : futures) {
                for(int i=0; i<60; i++) {
                    if(future != null && !future.isDone()) {
                        System.out.println("A future has NOT been done. Wait 5 seconds");
                        Thread.sleep(5000);
                    } else {
                        System.out.println("A future has been done. Ignore it");
                        break;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Finished re-indexing. (" + (new Date()) + ")");
        indexTool.shutdown();
    }

    private void updateIndexForPids(String pidFilePath) {
        InputStream pidFileStream = openPidFile(pidFilePath);
        if (pidFileStream != null) {
            
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(pidFileStream,
                        Charset.forName("UTF-8")));
                
                String line = null;
                while ((line = br.readLine()) != null) {
                    createIndexTaskForPid(StringUtils.trim(line));
            }
                System.out.println("All tasks generated, now updating index....");
                processIndexTasks();
                System.out.println("Index update complete.");
            } catch (IOException e) {
                System.out.println("Error reading line from pid file");
                return;
            } finally {
                IOUtils.closeQuietly(pidFileStream);
            }
        }
    }

    private void createIndexTaskForPid(String pid) {
        if (StringUtils.isNotEmpty(pid)) {
            Identifier identifier = new Identifier();
            identifier.setValue(pid);
            SystemMetadata smd = systemMetadata.get(identifier);
            if (smd == null) {
                System.out.println("Unable to get system metadata for id: " + pid);
            } else {
                String objectPath = retrieveObjectPath(smd.getIdentifier().getValue());
                generator.processSystemMetaDataUpdate(smd, objectPath);
                System.out.println("Created index task for id: " + pid);
            }
        }
    }

    // if dateParameter is null -- full refresh
    private void generateIndexTasksAndProcess(Date dateParameter, int totalToProcess, int startIndex) {
        System.out.print("Generating index updates: "+(new Date()));
        int count = 0;
        System.out.println("System Identifiers HzCast structure contains: " + pids.size()
                + " identifiers.");
        List<IndexTask> queue = new ArrayList<IndexTask> ();
        for (Identifier smdId : pids) {
            count++;
            if (count < startIndex) {
                System.out.println("Skipping pid: " + smdId.getValue());
                continue;
            }
            if (startIndex > 0) {
                startIndex = -1;
            }
            SystemMetadata smd = systemMetadata.get(smdId);
            if (dateParameter == null
                    || dateParameter.compareTo(smd.getDateSysMetadataModified()) <= 0) {

                if (smd == null || smd.getIdentifier() == null) {
                    System.out.println("PID: " + smdId.getValue()
                            + " exists in pids set but cannot be found in system metadata map.");
                } else {
                    String objectPath = retrieveObjectPath(smd.getIdentifier().getValue());
                    //create a task on the memory and doesn't save them into the db
                    IndexTask task = new IndexTask(smd, objectPath);
                    task.setAddPriority();
                    queue.add(task);
                    //generator.processSystemMetaDataUpdate(smd, objectPath);
                    
                    if (count > INDEX_TASK_ONE_CYCLE_SIZE) {
                        //processIndexTasks();
                        processor.processIndexTaskQueue(queue);
                        count = 0;
                        logger.info("SolrINdexBuildTool.generateIndexTasksAndProcess - empty the queue for the next cycle.");
                        queue = new ArrayList<IndexTask> ();
                    }
                    if (count % 10 == 0) {
                        System.out.print(".");
                    }
                    if (totalToProcess > 0 && count >= totalToProcess) {
                        System.out.print("Total to process reached. Exiting after processing.");
                        break;
                    }
                }
            }
        }
        //System.out.println("Finished generating index update."+(new Date()));
        //System.out.println("Processing index task requests.");
        // call processor:
        // it won't be called on last iteration of the for loop if count < 1000
        //processIndexTasks();
        processor.processIndexTaskQueue(queue);
        //System.out.println("Finished processing index task requests.");
        //finally we try to process failed index task again
        processor.processFailedIndexTaskQueue();
    }

    private void processIndexTasks() {
        if (BATCH_UPDATE)
            processor.batchProcessIndexTaskQueue();
        else
            processor.processIndexTaskQueue();
    }

    private String retrieveObjectPath(String pid) {
        Identifier PID = new Identifier();
        PID.setValue(pid);
        return objectPaths.get(PID);
    }

    private void configureHazelcast() {
        logger.info("starting hazelcast client...");
        hzClient = HazelcastClientFactory.getStorageClient();
        systemMetadata = hzClient.getMap(HZ_SYSTEM_METADATA);
        objectPaths = hzClient.getMap(HZ_OBJECT_PATH);
        pids = hzClient.getSet(HZ_IDENTIFIERS);
    }

    private void configureContext() {
        if (buildNextIndex) {
            context = new ClassPathXmlApplicationContext(NEXT_INDEX_APPLICATION_CONTEXT);
        } else {
            context = new ClassPathXmlApplicationContext(DEFAULT_INDEX_APPLICATION_CONTEXT);
        }
        generator = (IndexTaskGenerator) context.getBean("indexTaskGenerator");
        processor = (IndexTaskProcessor) context.getBean("indexTaskProcessor");
    }

    private void shutdown() {
        hzClient.shutdown();
    }

    private static void showHelp() {
        System.out.println("DataONE solr index build tool help:");
        System.out.println(" ");
        System.out.println("This tool indexes objects the CN's system metadata map.");
        System.out.println("   Nothing is removed from the solr index, just added/updated.");
        System.out.println(" ");
        System.out.println("Please stop the d1-index-task-processor while this tool runs: ");
        System.out.println("       /etc/init.d/d1-index-task-processor stop");
        System.out.println("And restart whent the tool finishes:");
        System.out.println("       /etc/init.d/d1-index-task-processor start");
        System.out.println(" ");
        System.out.println("-d     System data modified date to begin index build/refresh from.");
        System.out.println("       Data objects modified/added after this date will be indexed.");
        System.out.println("       Date format: mm/dd/yyyy.");
        System.out.println(" ");
        System.out.println("-a     Build/refresh all data objects regardless of modified date.");
        System.out.println(" ");
        System.out
                .println("-c     Build/refresh a number data objects, the number configured by this option.");
        System.out.println("        This option is primarily intended for testing purposes.");
        System.out.println(" ");
        System.out
                .println("-startAt  Build/refresh objects, starting at this index in the hazelcast Identifiers Set.");
        System.out.println(" ");
        System.out
                .println("-pidFile   Refresh index document for pids contained in the file path ");
        System.out
                .println("           supplied with this option.  File should contain one pid per line.");
        System.out.println(" ");
        System.out.println("-migrate   Build/refresh data object into the next search index");
        System.out.println("             version's core - as configured in: ");
        System.out.println("              /etc/dataone/solr-next.properties");
        System.out.println("Exactly one option amoung -d or -a or -pidFile must be specified.");
    }

    private void setBuildNextIndex(boolean next) {
        this.buildNextIndex = next;
    }

    private InputStream openPidFile(String pidFilePath) {
        InputStream pidFileStream = null;
        try {
            pidFileStream = new FileInputStream(pidFilePath);
        } catch (FileNotFoundException e) {
            System.out.println("Unable to open file at: " + pidFilePath + ".  Exiting.");
        }
        return pidFileStream;
    }
    
    public IndexTaskProcessor getIndexTaskProcessor() {
        return processor;
    }
}
