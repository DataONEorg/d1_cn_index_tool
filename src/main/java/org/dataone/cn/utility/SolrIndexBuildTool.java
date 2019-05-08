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
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.jena.atlas.logging.Log;
import org.apache.log4j.Logger;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.cn.index.generator.IndexTaskGenerator;
import org.dataone.cn.index.processor.IndexTaskProcessor;
import org.dataone.cn.index.task.IgnoringIndexIdPool;
import org.dataone.cn.index.task.IndexTask;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.TypeFactory;
import org.dataone.service.types.v2.SystemMetadata;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.IMap;

import static java.util.stream.Collectors.toCollection;

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
 
        // try and run before shutdown
//        Runtime.getRuntime().addShutdownHook(new Thread() {
//            public void run() {
//                System.out.println("Shutdown Hook is running");
//            }
//        });
        
        
     // create Options object
        Options options = new Options();
        options.addOption("help", false, "print this message");

        options.addOption("getCount", false, "print this message");
        options.addOption("listPids", false, "Output a list of all pids");
        options.addOption("all", false, "reindex from HZ identifiers set from the object store");        
        options.addOption("pidFile", true, "Refresh index document for pids contained in the file " +
        		"path supplied with this option.  File should contain one pid per line.");

        options.addOption("date", true, "System data modified date to begin index build/refresh from. Format: mm/dd/yyyy");
        options.addOption("startAt", true, "index in the list to start processing at");
        options.addOption("count", true, "the number of items to process");

        options.addOption("useIndexQueue", false, "use the persistent index task queue to process from.  this simulates normal workflow");
        options.addOption("generateOnly", false, "Don't process any tasks, just submit to the persistent index task queue.");
        options.addOption("processOnly", false, "Don't generate new tasks");
        options.addOption("migrate", false, "Build/refresh data object into the next search index version's core " +
        		"- as configured in: /etc/dataone/solr-next.properties");
        options.addOption("stayAlive", false, "if set, keeps process alive until interrupt received.  Use primarily for profiling.");
        
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            // parse the command line arguments
            cmd = parser.parse( options, args );
        } catch (org.apache.commons.cli.ParseException e) {
         // oops, something went wrong
            System.err.println( "Command Line Parsing failed.  Reason: " + e.getMessage() );
        } 
        
        HelpFormatter formatter = new HelpFormatter();
        if (cmd.hasOption("help")) {
            // automatically generate the help statement
             formatter.printHelp( "index build tool", options );
            return;
        }
        
       
        String[] exclusiveOptions = new String[]{"all","pidList","getCount","listPids"};
        int excl = 0;
        for (int i=0; i<exclusiveOptions.length; i++) {
            if (cmd.hasOption(exclusiveOptions[i])) 
                excl++;
        }
        if (excl > 1) {
            System.err.println("Only one of -all, -pidFile, -getCount, or -listPids can be used!!");
            // automatically generate the help statement
            formatter.printHelp( "index build tool", options );
            return;
        }
        // (zero exclusive options are ok too)
        
        exclusiveOptions = new String[]{"processOnly","generateOnly","useIndexQueue"};
        excl = 0;
        for (int i=0; i<exclusiveOptions.length; i++) {
            if (cmd.hasOption(exclusiveOptions[i])) 
                excl++;
        }
        if (excl > 1) {
            System.err.println("Only one of -processOnly, -generateOnly, -useIndexQueue,can be used!!");
            // automatically generate the help statement
            formatter.printHelp( "index build tool", options );
            return;
        }
        // (zero exclusive options are ok too)
        
        
        // validate Date value
        DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
        Date startDate = null;
        String dateString = null;
        if (cmd.hasOption("date")) {
            try {
                startDate = dateFormat.parse(cmd.getOptionValue("date"));
            } catch (ParseException e) {
                System.out.println("Unable to parse provided date string: " + dateString);
                // automatically generate the help statement
                formatter.printHelp( "index build tool", options );
               return; 
            }
        }
        
        SolrIndexBuildTool indexTool = new SolrIndexBuildTool();
        
        try {
            indexTool.doWork(cmd);
        } catch (Exception e) {
            System.out.println("Solr index refresh failed: " + e.getMessage());
            e.printStackTrace(System.out);
        }
        if (cmd.hasOption("stayAlive")) {
            
            while (true) {
                try {
                    Thread.sleep(60000);
                } catch (InterruptedException e) {
                    System.out.println("Interrupt received...");
                    break;
                }
            }
        }
        System.out.println("Exiting solr index refresh tool.");
    }
    
    /**
     * the main processing routine
     * 
     * @param indexTool
     * @param cmd
     * @throws InterruptedException 
     */
    public void doWork(CommandLine cmd) throws InterruptedException  {
        
        setBuildNextIndex(cmd.hasOption("migrate"));
        configureContext();
        
        System.err.println("Starting work... (" + (new Date()) + ")");
        
    
        configureHazelcast();

        if (cmd.hasOption("getCount")) {
            System.out.println("There is a pid total (from HZ Identifier set) of " + pids.size());
            return;
        } 
        
        if (cmd.hasOption("listPids")) {
            System.err.println("Listing Pids (from Hazelcast Identifiers set)");
            for (Identifier pid : pids) {
                System.out.println(pid.getValue());
            }
            return;
        }
        
        
        boolean exceptionThrown = false;
        int futuresCount = 0;
        Set<Future> doneFutures = new HashSet<>();
        try{

            /// get list iterator from source
            Collection<Identifier> pidSource= null;
            
            
            CHOOSE_SOURCE:
                if (cmd.hasOption("all")) {

                    System.err.println("Reindexing all from HZ.identifiers map, using date and index filters");
                    pidSource = pids;

                } else if (cmd.hasOption("pidFile")) {

                    String filepath = cmd.getOptionValue("pidFile");
                    pidSource = getPidList(filepath);
                    
                }
          
            List<Identifier> filteredPids = null;
            
            SLICE_SUBSET:
                if  (pidSource != null) { 
                   
                    Stream s = pidSource.stream();

                    System.out.println("start  / count = " + cmd.getOptionValue("startAt") + " / " + cmd.getOptionValue("count"));
                    if (cmd.hasOption("startAt")) 
                        s = s.skip(Long.valueOf(cmd.getOptionValue("startAt")));

                    if (cmd.hasOption("count"))
                        s = s.limit(Long.valueOf(cmd.getOptionValue("count")));

                    filteredPids =  (List<Identifier>) s.collect(Collectors.toList());

                    System.out.println("********** post filtering pid count = " + filteredPids.size());
            
                } else if (!cmd.hasOption("processOnly")) {
                    return;
                }
            
            FILL_INDEX_TASK_QUEUE:
                if (cmd.hasOption("useIndexQueue") || cmd.hasOption("generateOnly")) {
                    // populate indexQueue from filtered list
                    for (Identifier id : filteredPids) {

                        SystemMetadata smd = systemMetadata.get(id);
                        if (smd == null) {
                            System.out.println("Unable to get system metadata for id: " + id.getValue());
                            continue;
                        }

                        if (!IgnoringIndexIdPool.isNotIgnorePid(smd))  // don't you love the double negative? 
                            continue;

                        String objectPath = retrieveObjectPath(smd.getIdentifier().getValue());

                        generator.processSystemMetaDataUpdate(smd, objectPath);
                        System.out.println("Submitted index task for id: " + id.getValue());
                    }

                }
                if (cmd.hasOption("generateOnly")) {
                    return;
                }
                
            RUN_PROCESSOR:    
                
                if  (cmd.hasOption("useIndexQueue") || cmd.hasOption("processOnly")) {
                    processor.processIndexTaskQueue();
               
                } else {
                   // create a private queue to run the processor with
                    List<IndexTask> privateQueue = new ArrayList<>();
                    for (Identifier id : filteredPids) {
                        
                        SystemMetadata smd = systemMetadata.get(id);
                        if (smd == null) {
                            System.out.println("Unable to get system metadata for id: " + id.getValue());
                            continue;
                        }

                        if (!IgnoringIndexIdPool.isNotIgnorePid(smd))  // don't you love the double negative? 
                            continue;

                        String objectPath = retrieveObjectPath(smd.getIdentifier().getValue());

                        IndexTask task = new IndexTask(smd, objectPath);
                        task.setAddPriority();
                        privateQueue.add(task); 
                    }

                    processor.processIndexTaskQueue(privateQueue);
                }
            

            ////  WAIT FOR TASKS TO COMPLETE 
            ////      examine the futures to help not wait too long

            Queue<Future> futures = getIndexTaskProcessor().getFutureQueue();
            futuresCount = futures.size(); // used in finally block
            System.out.println(futures.size() + " futures found in the indexProcessorFutureQueue.");
            if (futuresCount > 0) {
                int totalTimeout = futuresCount * 2000; 
                System.out.println("... waiting maximum of " + totalTimeout + "ms to finish (2s per future...");
                
                long start = System.currentTimeMillis();
                while (System.currentTimeMillis() < start + totalTimeout) {

                    for(Future future : futures) {

                        if (!doneFutures.contains(future) && future.isDone()) {
                            doneFutures.add(future);
                            try {
                                future.get(100, TimeUnit.MILLISECONDS);
                            } catch (Throwable t) {
                                Throwable cause = t.getCause();
                                if (cause == null) {
                                    cause = t;
                                }
                                logger.warn("Exception returned from the thread: " + cause.getClass().getSimpleName() + ":: " + cause.getMessage());
                                t.printStackTrace();
                            }
                        }
                    }
                    if (doneFutures.size() == futuresCount) {
                        int coolDownMillis = futuresCount > 9000 ? futuresCount/3 : 3000;
                        System.out.println("all futures are done (" + futuresCount + "). Cooling-down for " + coolDownMillis + " millis.");
                        Thread.sleep(coolDownMillis);  // this is a cool-down period 
                        break;
                    }

                    Thread.sleep(2000);
                    System.out.println("Total of " + doneFutures.size() + " futures of " + futures.size() + " are done.");
                }
                if (doneFutures.size() < futuresCount) {
                    System.out.println("Not all futures completed before timing out. There are " + (futuresCount - doneFutures.size())
                            + " not done.");
                }

            }

        } catch (Throwable e) {
            e.printStackTrace();
            exceptionThrown = true;
            System.out.print("Throwable thrown. Recalculating futureQueue.size from the executor.  Was " + futuresCount);
            try {
                futuresCount = getIndexTaskProcessor().getFutureQueue().size();
                System.out.println(".  Now " + futuresCount);
            } catch (Throwable t) {
                ;
            }
            System.out.println("Exception thrown during processing, so waiting the maximum time (" 
                    + 2 * futuresCount + " seconds) before shutting down");
            
            long startWait = System.currentTimeMillis();
            try {
                Thread.sleep(futuresCount * 2000);
            } catch (InterruptedException e1) {
                System.out.println("Are you sure you want to interrupt?  Signal again to break out of waiting period.");
                long elapsed = System.currentTimeMillis() - startWait;
                Thread.sleep(futuresCount * 2000 - elapsed);
                
            }
        } 
        finally {
            if (doneFutures.size() < futuresCount) {
                logger.warn("Shutting down index task processor executor...");
                getIndexTaskProcessor().shutdownExecutor();
            }
            logger.warn("Finishing work... (" + (new Date()) + ")");
            System.out.println("Finishing work... (" + (new Date()) + ")");
            shutdown();
            
        }
    }



    /**
     * returns a list of pids from the provided file,
     * trimming leading and trailing whitespace,
     * filtering out pids with internal whitespace and blank lines
     * 
     * @param filepath
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     */
    private List<Identifier> getPidList(String filepath) throws FileNotFoundException, IOException {
        
        try (InputStream pidFileStream  = new FileInputStream(filepath);
             BufferedReader br = new BufferedReader(
             new InputStreamReader(pidFileStream, Charset.forName("UTF-8")))
                ) 
        {
            
            List<Identifier> pidList = new ArrayList<Identifier>();    

            System.out.println("Generating tasks from pid list...");
            String line = null;
            while ((line = br.readLine()) != null) {
                String trimmed = StringUtils.trimToEmpty(line);
                
                // filter out empty and whitespace containing lines
                if (trimmed.matches(".+\\s.+"))
                    ;
                else if (trimmed.length() == 0 )
                    ;
                else
                    pidList.add(TypeFactory.buildIdentifier(trimmed));
            }
            return pidList;
        } 
    }

    private void updateIndexForPids(String pidFilePath) {
        
        
        ////// GENERATE .....
        
//        InputStream pidFileStream = null;
//        try {
//            if (true /*skipGenerating*/) {
//                System.out.println("Skipping task generation, as per -skipGenerating argument.");
//            } else {
//                pidFileStream = new FileInputStream(pidFilePath);
//                BufferedReader br = new BufferedReader(new InputStreamReader(pidFileStream, Charset.forName("UTF-8")));
//            
//                System.out.println("Generating tasks from pid list...");
//                String line = null;
//                while ((line = br.readLine()) != null) {
//                    createIndexTaskForPid(StringUtils.trim(line));
//                }
//                System.out.println("All tasks generated...");
//            }
//                
//        } catch (IOException e) {
//            System.out.println("Error reading line from pid file");
//            return;
//        } finally {
//            IOUtils.closeQuietly(pidFileStream);
//        }

        
        ////// PROCESS .....
        
//        if (true /*skipProcessing*/) {
//            System.out.println("Skipping processing, as per -skipProcessing argument.");
//        } else {
//            System.out.println("Starting index processor...");
//            processor.processIndexTaskQueue();
//            System.out.println("Index processor execution complete...");
//        } 
    }

    
    
    
//    private void createIndexTaskForPid(String pid) {
//        if (StringUtils.isNotEmpty(pid)) {
//            Identifier identifier = new Identifier();
//            identifier.setValue(pid);
//            SystemMetadata smd = systemMetadata.get(identifier);
//            if (smd == null) {
//                System.out.println("Unable to get system metadata for id: " + pid);
//            } else {
//                String objectPath = retrieveObjectPath(smd.getIdentifier().getValue());
//                generator.processSystemMetaDataUpdate(smd, objectPath);
//                System.out.println("Created index task for id: " + pid);
//            }
//        }
//    }

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

//    private void processIndexTasks() {
//        /*if (BATCH_UPDATE)
//            processor.batchProcessIndexTaskQueue();
//        else*/
//            processor.processIndexTaskQueue();
//    }

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

//    private static void showHelp() {
//        System.out.println("DataONE solr index build tool help:");
//        System.out.println(" ");
//        System.out.println("This tool indexes objects the CN's system metadata map.");
//        System.out.println("   Nothing is removed from the solr index, just added/updated.");
//        System.out.println(" ");
//        System.out.println("Please stop the d1-index-task-processor while this tool runs: ");
//        System.out.println("       /etc/init.d/d1-index-task-processor stop");
//        System.out.println("And restart whent the tool finishes:");
//        System.out.println("       /etc/init.d/d1-index-task-processor start");
//        System.out.println(" ");
//        System.out.println("-date=     System data modified date to begin index build/refresh from.");
//        System.out.println("             Data objects modified/added after this date will be indexed.");
//        System.out.println("             Date format: mm/dd/yyyy.");
//        System.out.println(" ");
//        System.out.println("-all       Build/refresh all data objects regardless of modified date.");
//        System.out.println(" ");
//        System.out.println("-startAt=  Build/refresh objects, starting at this index in the hazelcast Identifiers Set.");
//        System.out.println(" ");
//        System.out.println("-pidFile=   ");
//        System.out.println("             Refresh index document for pids contained in the file path supplied with this option.  File should contain one pid per line.");
//        System.out.println(" ");
//        System.out.println("-migrate   Build/refresh data object into the next search index");
//        System.out.println("             ");
//        System.out.println("              Build/refresh data object into the next search index version's core - as configured in: /etc/dataone/solr-next.properties");
//        System.out.println(" ");
//        System.out.println("-count=    Build/refresh a number data objects, the number configured by this option.");
//        System.out.println("             This option is primarily intended for testing purposes.");
//        System.out.println(" ");
//        System.out.println("-skipProcessing   Don't process tasks");
//        System.out.println(" ");
//        System.out.println("-skipGenerating   Don't generate new tasks");
//        System.out.println(" ");
//        System.out.println("-getCount       Only get a count of how many the -all option will process");
//        System.out.println(" ");
//        System.out.println("-listPids       Output a list of all pids");
//        System.out.println(" ");
//        
//        
//        System.out.println("Exactly one option among -date, -all, -pidFile, or -startAt must be specified.");
//    }

    private void setBuildNextIndex(boolean next) {
        this.buildNextIndex = next;
    }

//    private InputStream openPidFile(String pidFilePath) {
//        InputStream pidFileStream = null;
//        try {
//            pidFileStream = new FileInputStream(pidFilePath);
//        } catch (FileNotFoundException e) {
//            System.out.println("Unable to open file at: " + pidFilePath + ".  Exiting.");
//        }
//        return pidFileStream;
//    }
    
    public IndexTaskProcessor getIndexTaskProcessor() {
        return processor;
    }
}
