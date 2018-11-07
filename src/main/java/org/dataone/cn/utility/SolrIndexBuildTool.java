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
import org.dataone.cn.index.task.IgnoringIndexIdPool;
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
    private static boolean skipProcessing = false;
    private static boolean skipGenerating = false;
    private static boolean getAllCount = false;
    private static boolean createPidList = false;

    public SolrIndexBuildTool() {
    }

    public static void main(String[] args) {
        DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
        Date startDate = null;
        String dateString = null;
        boolean help = false;
        boolean all = false;
        boolean migrate = false;
        
        String pidFile = null;
        int totalToProcess = 0;
        int startIndex = 0;
        int options = 0;
        int modeOptions = 0;
        for (String arg : args) {
            if (StringUtils.startsWith(arg, "-date=")) {
                dateString = StringUtils.substringAfter(arg, "-date=");
                dateString = StringUtils.trim(dateString);
                try {
                    startDate = dateFormat.parse(dateString);
                    options++;
                    modeOptions++;
                } catch (ParseException e) {
                    System.out.println("Unable to parse provided date string: " + dateString);
                }
            } else if (StringUtils.startsWith(arg, "-help")) {
                help = true;
            } else if (StringUtils.startsWith(arg, "-all")) {
                all = true;
                options++;
                modeOptions++;
            } else if (StringUtils.startsWith(arg, "-getCount")) {
                getAllCount = true;
                options++;
                modeOptions++;
            } else if (StringUtils.startsWith(arg, "-migrate")) {
                migrate = true;
            } else if (StringUtils.startsWith(arg, "-pidFile=")) {
                pidFile = StringUtils.trim(StringUtils.substringAfter(arg, "-pidFile="));
                options++;
                modeOptions++;
            } else if (StringUtils.startsWith(arg, "-startAt=")) {
                options++;
                String startAt = StringUtils.trim(StringUtils.substringAfter(arg, "-startAt="));
                startIndex = Integer.valueOf(startAt).intValue();
            } else if (StringUtils.startsWith(arg, "-count=")) {
                String countStr = StringUtils.trim(StringUtils.substringAfter(arg, "-count="));
                totalToProcess = Integer.valueOf(countStr).intValue();
                options++;
            } else if (StringUtils.startsWith(arg, "-skipProcessing")) {
                skipProcessing = true;
                options++;
            } else if (StringUtils.startsWith(arg, "-skipGenerating")) {
                skipGenerating = true;
                options++;
            } else if (StringUtils.startsWith(arg, "-listPids")) {
                createPidList = true;
                options++;
                modeOptions++;
            }
        }

        if (help) {
            showHelp();
            return;
        }
        if (modeOptions != 1) {
            System.out
                    .println("Exactly one option amoung -all, -date, -pidFile, -startAt may be used at a time.");
            showHelp();
            return;
        } 
        if (all) {
            System.out.println("Performing full build/refresh of solr index.");
        } 
        else if (getAllCount) {
            System.out.println("Getting count of all objects");
        }
        else if (createPidList) {
            System.out.println("PidList");
        }
        else if (startDate != null) {
            System.out.println("Performing (re)build from date: " + dateFormat.format(startDate) + ".");
        } 
        else if (pidFile != null) {
            System.out.println("Performing refresh/index for pids found in file: " + pidFile);
        }
        else if (startIndex > 0) {
            System.out.println("Performing index/refresh from hazelcast Identifier set index: "+ startIndex);
        }

        if (migrate) {
            System.out.println("Refreash targeting the next version of the search index");
        } else {
            System.out.println("Refresh targeting the current live search index.");
        }
        
        if (totalToProcess > 0) {
            System.out.println("Limiting refresh to " + totalToProcess + " items.");
        }
        
        if (skipProcessing) {
            System.out.println("skipProcessing flag is set...Tasks will be processed when index task processor " +
            		"is started up or a future run without this flag");
        }
        if (skipGenerating) {
            System.out.println("skipGenerating flag is set...Only existing tasks will be processed");
        }
        
        
        
        System.out.println(" ");

        SolrIndexBuildTool indexTool = new SolrIndexBuildTool();
        indexTool.setBuildNextIndex(migrate);
        try {
            refreshSolrIndex(indexTool, startDate, pidFile, totalToProcess, startIndex);
        } catch (Exception e) {
            System.out.println("Solr index refresh failed: " + e.getMessage());
            e.printStackTrace(System.out);
        }

        System.out.println("Exiting solr index refresh tool.");
        System.exit(0);
    }

    /**
     * The main processing routine
     * 
     * @param indexTool
     * @param dateParameter
     * @param pidFilePath
     * @param totalToProcess
     * @param startIndex
     */
    private static void refreshSolrIndex(SolrIndexBuildTool indexTool, Date fromDate,
            String pidFilePath, int totalToProcess, int startIndex) {
        indexTool.configureContext();
        
        System.err.println("Starting work... (" + (new Date()) + ")");
        
        try {
            indexTool.configureHazelcast();
            
            if (getAllCount) {
                System.out.println("There is a pid total of " + indexTool.pids.size());
                return;
            } 
            else if (createPidList) {
                System.err.println("Listing Pids (from Hazelcast Identifiers set)");
                for (Identifier pid : indexTool.pids) {
                    System.out.println(pid.getValue());
                }
                return;
            }
            else if (pidFilePath == null) {
                System.err.println("Reindexing all with date and index filters");
                indexTool.generateIndexTasksAndProcess(fromDate, totalToProcess, startIndex);
            } 
            else {
                System.err.println("Reindexing from pidFile");
                indexTool.updateIndexForPids(pidFilePath);
            }
            try {
                Queue<Future> futures = indexTool.getIndexTaskProcessor().getFutureQueue();
                if (futures != null && futures.size() > 0) {
                    for(Future future : futures) {
                        for(int i=0; i<60; i++) {
                            if(future != null && !future.isDone()) {
                                logger.info("A future has NOT been done. Wait 5 seconds to shut down the index tool.");
                                Thread.sleep(5000);
                            } else {
                                logger.info("A future has been done. Ignore it before shutting down the index tool.");
                                break;
                            }
                        }
                    }
                }
                indexTool.getIndexTaskProcessor().shutdownExecutor();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {

            }

        } finally {
            logger.warn("Shutting down index task processor executor...");
            indexTool.getIndexTaskProcessor().shutdownExecutor();
            logger.warn("Finishing work... (" + (new Date()) + ")");
            System.out.println("Finishing work... (" + (new Date()) + ")");
            indexTool.shutdown();
        }
    }

    private void updateIndexForPids(String pidFilePath) {
        
        
        ////// GENERATE .....
        
        InputStream pidFileStream = null;
        try {
            if (skipGenerating) {
                System.out.println("Skipping task generation, as per -skipGenerating argument.");
            } else {
                pidFileStream = new FileInputStream(pidFilePath);
                BufferedReader br = new BufferedReader(new InputStreamReader(pidFileStream, Charset.forName("UTF-8")));
            
                System.out.println("Generating tasks from pid list...");
                String line = null;
                while ((line = br.readLine()) != null) {
                    createIndexTaskForPid(StringUtils.trim(line));
                }
                System.out.println("All tasks generated...");
            }
                
        } catch (IOException e) {
            System.out.println("Error reading line from pid file");
            return;
        } finally {
            IOUtils.closeQuietly(pidFileStream);
        }

        
        ////// PROCESS .....
        
        if (skipProcessing) {
            System.out.println("Skipping processing, as per -skipProcessing argument.");
        } else {
            System.out.println("Starting index processor...");
            processIndexTasks();
            System.out.println("Index processor execution complete...");
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

    private void generateIndexTasksAndProcess(Date fromDate, int totalToProcess, int startIndex) {
        System.out.println("Generating index updates: "+(new Date()));
        int count = 0;
        System.out.println("System Identifiers HzCast structure contains: " + pids.size()
                + " identifiers.");
        List<IndexTask> queue = new ArrayList<IndexTask> ();
        for (Identifier smdId : pids) {
            SystemMetadata smd = systemMetadata.get(smdId);
            if(!IgnoringIndexIdPool.isNotIgnorePid(smd)) {
                //we should skip the pid since it is not supposed to be indexed.
                System.out.println("PID: " + smdId.getValue()
                        + " was skipped for indexing since it is in the ignoring id pool.");
                continue;
            }
            count++;
            if (count < startIndex) {
                System.out.println("Skipping pid: " + smdId.getValue());
                continue;
            }
            if (startIndex > 0) {
                startIndex = -1;
            }
            
            if (fromDate == null
                    || fromDate.compareTo(smd.getDateSysMetadataModified()) <= 0) {

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
                        System.out.println("Total to process reached. Exiting after processing.");
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
        logger.info("Submitting all new index tasks has completed in the generaterIndexTasksAndProcess");
        //wait until previous indexing to be finished
        try {
            Queue<Future> futures = getIndexTaskProcessor().getFutureQueue();
            for(Future future : futures) {
                for(int i=0; i<60; i++) {
                    if(future != null && !future.isDone()) {
                        logger.info("A future has NOT been done. Wait 5 seconds for starting to index failed index tasks.");
                        Thread.sleep(5000);
                    } else {
                        logger.info("A future has been done. Ignore it before starting to index failed index tasks.");
                        break;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        //System.out.println("Finished processing index task requests.");
        logger.info("All new index tasks have been done in the generaterIndexTasksAndProcess and we will start to index the failured or not-ready index tasks.");
        //finally we try to process some new or failed index tasks generated in above process again
        processor.processIndexTaskQueue();
    }

    private void processIndexTasks() {
        /*if (BATCH_UPDATE)
            processor.batchProcessIndexTaskQueue();
        else*/
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
        
        if (hzClient != null) {
            logger.warn("Shutting down HZ client...");
            hzClient.shutdown();
        } else {
            logger.warn("(No HZ client to shutdown)");
        }
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
        System.out.println("-date=     System data modified date to begin index build/refresh from.");
        System.out.println("             Data objects modified/added after this date will be indexed.");
        System.out.println("             Date format: mm/dd/yyyy.");
        System.out.println(" ");
        System.out.println("-all       Build/refresh all data objects regardless of modified date.");
        System.out.println(" ");
        System.out.println("-startAt=  Build/refresh objects, starting at this index in the hazelcast Identifiers Set.");
        System.out.println(" ");
        System.out.println("-pidFile=  Refresh index document for pids contained in the file path ");
        System.out.println("             supplied with this option.  File should contain one pid per line.");
        System.out.println(" ");
        System.out.println("-migrate   Build/refresh data object into the next search index");
        System.out.println("             version's core - as configured in: ");
        System.out.println("              /etc/dataone/solr-next.properties");
        System.out.println(" ");
        System.out.println("-count=    Build/refresh a number data objects, the number configured by this option.");
        System.out.println("             This option is primarily intended for testing purposes.");
        System.out.println(" ");
        System.out.println("-skipProcessing   Don't process tasks");
        System.out.println(" ");
        System.out.println("-skipGenerating   Don't generate new tasks");
        System.out.println(" ");
        System.out.println("-getCount       Only get a count of how many the -all option will process");
        System.out.println(" ");
        System.out.println("-listPids       Output a list of all pids");
        System.out.println(" ");
        
        
        System.out.println("Exactly one option among -date, -all, -pidFile, or -startAt must be specified.");
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
