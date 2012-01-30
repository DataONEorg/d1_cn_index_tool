package org.dataone.cn.utility;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.dataone.cn.hazelcast.HazelcastClientInstance;
import org.dataone.cn.index.generator.IndexTaskGenerator;
import org.dataone.cn.index.processor.IndexTaskProcessor;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.SystemMetadata;
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

    private HazelcastClient hzClient;
    private IMap<Identifier, SystemMetadata> systemMetadata;
    private IMap<Identifier, String> objectPaths;
    private Set<Identifier> pids;
    private ApplicationContext context;

    private IndexTaskGenerator generator;
    private IndexTaskProcessor processor;

    public SolrIndexBuildTool() {
    }

    public static void main(String[] args) {
        DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
        Date dateParameter = null;
        String dateString = null;
        boolean help = false;
        boolean fullRefresh = false;
        for (String arg : args) {
            if (StringUtils.startsWith(arg, "-d")) {
                dateString = StringUtils.substringAfter(arg, "-d");
                dateString = StringUtils.trim(dateString);
                try {
                    dateParameter = dateFormat.parse(dateString);
                } catch (ParseException e) {
                    System.out.println("Unable to parse provided date string: " + dateString);
                }
            } else if (StringUtils.startsWith(arg, "-help")) {
                help = true;
            } else if (StringUtils.startsWith(arg, "-a")) {
                fullRefresh = true;
            }
        }

        if (help || (fullRefresh == false && dateParameter == null)) {
            showHelp();
            return;
        } else if (fullRefresh == true && dateParameter != null) {
            System.out.println("Both -a and -d options provided, using date parameter: "
                    + dateString);
            fullRefresh = false;
        }
        if (fullRefresh) {
            System.out.println("Performing full build/refresh of solr index.");
        } else if (dateParameter != null) {
            System.out.println("Performing (re)build from date: "
                    + dateFormat.format(dateParameter) + ".");
        }

        System.out.println("Starting solr index refresh.");

        SolrIndexBuildTool indexTool = new SolrIndexBuildTool();

        try {
            refreshSolrIndex(indexTool, dateParameter);
        } catch (Exception e) {
            System.out.println("Solr index refresh failed: " + e.getMessage());
            e.printStackTrace(System.out);
        }

        System.out.println("Exiting solr index refresh tool.");
    }

    private static void refreshSolrIndex(SolrIndexBuildTool indexTool, Date dateParameter) {
        indexTool.configureContext();
        indexTool.configureHazelcast();
        indexTool.generateIndexTasksAndProcess(dateParameter);
        indexTool.shutdown();
    }

    // if dateParameter is null -- full refresh
    private void generateIndexTasksAndProcess(Date dateParameter) {
        System.out.print("Generating index updates: ");
        int count = 0;
        for (Identifier smdId : pids) {
            SystemMetadata smd = systemMetadata.get(smdId);
            if (dateParameter == null
                    || dateParameter.compareTo(smd.getDateSysMetadataModified()) <= 0) {
                String objectPath = retrieveObjectPath(smd.getIdentifier().getValue());
                generator.processSystemMetaDataUpdate(smd, objectPath);
                count++;
                if (count > 1000) {
                    processIndexTasks();
                    count = 0;
                }
                if (count % 10 == 0) {
                    System.out.print(".");
                }
            }
        }
        System.out.println("Finished generating index update.");
        System.out.println("Processing index task requests.");
        // call processor:
        // it won't be called on last iteration of the for loop if count < 1000
        processIndexTasks();
        // call processor a final time to process resource maps that could not
        // process on first pass - necessary?
        // processIndexTasks();
        System.out.println("Finished processing index task requests.");
    }

    private void processIndexTasks() {
        processor.processIndexTaskQueue();
    }

    private String retrieveObjectPath(String pid) {
        Identifier PID = new Identifier();
        PID.setValue(pid);
        return objectPaths.get(PID);
    }

    private void evict(Identifier smdId) {
        systemMetadata.evict(smdId);
        System.out.println("evicted: " + smdId.getValue());
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void configureHazelcast() {
        logger.info("starting hazelcast client...");
        hzClient = HazelcastClientInstance.getHazelcastClient();
        systemMetadata = hzClient.getMap(HZ_SYSTEM_METADATA);
        objectPaths = hzClient.getMap(HZ_OBJECT_PATH);
        pids = hzClient.getSet(HZ_IDENTIFIERS);
    }

    private void configureContext() {
        context = new ClassPathXmlApplicationContext("index-tool-context.xml");
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
        System.out.println("Please stop the d1-cn-index-processor while this tool runs: ");
        System.out.println("       /etc/init.d/d1-cn-index-processor stop");
        System.out.println("And restart whent the tool finishes:");
        System.out.println("       /etc/init.d/d1-cn-index-processor start");
        System.out.println(" ");
        System.out.println("-d     System data modified date to begin index build/refresh from.");
        System.out.println("       Data objects modified/added after this date will be indexed.");
        System.out.println("       Date format: mm/dd/yyyy.");
        System.out.println(" ");
        System.out.println("-a     Build/refresh all data objects regardless of modified date.");
        System.out.println(" ");
        System.out.println("Either -d or -a option must be specified.");
    }
}
