package org.dataone.cn.utility;

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

    private HazelcastClient hzClient;
    private IMap<Identifier, SystemMetadata> systemMetadata;
    private IMap<Identifier, String> objectPaths;
    private ApplicationContext context;

    private IndexTaskGenerator generator;
    private IndexTaskProcessor processor;

    public SolrIndexBuildTool() {
    }

    public static void main(String[] args) {
        System.out.println("Starting solr index refresh.");
        SolrIndexBuildTool indexTool = new SolrIndexBuildTool();

        try {
            refreshSolrIndex(indexTool);
        } catch (Exception e) {
            System.out.println("Solr index refresh failed: " + e.getMessage());
            e.printStackTrace(System.out);
        }

        System.out.println("Exiting solr index refresh tool.");
    }

    private static void refreshSolrIndex(SolrIndexBuildTool indexTool) {
        indexTool.configureContext();
        indexTool.configureHazelcast();
        indexTool.generateIndexTasksAndProcess();
        // call processor a final time to process resource maps that could not
        // process on first pass (references not indexed delays resource map
        // indexing)
        indexTool.processIndexTasks();
    }

    private void generateIndexTasksAndProcess() {
        int count = 0;
        for (SystemMetadata smd : systemMetadata.values()) {
            String objectPath = retrieveObjectPath(smd.getIdentifier().getValue());
            generator.processSystemMetaDataUpdate(smd, objectPath);
            count++;
            if (count > 1000) {
                processIndexTasks();
                count = 0;
            }
        }
        // call processor:
        // it won't be called on last iteration of the for loop if count < 1000
        processIndexTasks();
    }

    private void processIndexTasks() {
        processor.processIndexTaskQueue();
    }

    private String retrieveObjectPath(String pid) {
        Identifier PID = new Identifier();
        PID.setValue(pid);
        return objectPaths.get(PID);
    }

    private void configureHazelcast() {
        logger.info("starting hazelcast client...");
        hzClient = HazelcastClientInstance.getHazelcastClient();
        systemMetadata = hzClient.getMap(HZ_SYSTEM_METADATA);
        objectPaths = hzClient.getMap(HZ_OBJECT_PATH);
    }

    private void configureContext() {
        context = new ClassPathXmlApplicationContext("index-tool-context.xml");
        generator = (IndexTaskGenerator) context.getBean("indexTaskGenerator");
        processor = (IndexTaskProcessor) context.getBean("indexTaskProcessor");
    }
}
