package org.dataone.cn.utility.test;

import static org.junit.Assert.fail;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.dataone.cn.utility.SolrIndexBuildTool;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

@RunWith(SpringJUnit4ClassRunner.class)
// context files are located from the root of the test's classpath
// for example org/dataone/cn/index/test/
@ContextConfiguration(locations = { "test-context.xml" })
public class SolrIndexBuildToolTest {

    private static Logger logger = Logger.getLogger(SolrIndexBuildToolTest.class.getName());

    private HazelcastInstance hzMember;
    private IMap<Identifier, SystemMetadata> sysMetaMap;
    private IMap<Identifier, String> objectPaths;

    private static final String systemMetadataMapName = Settings.getConfiguration().getString(
            "dataone.hazelcast.systemMetadata");

    private static final String objectPathName = Settings.getConfiguration().getString(
            "dataone.hazelcast.objectPath");

    @Autowired
    private Resource systemMetadataResource1;
    @Autowired
    private Resource systemMetadataResource2;
    @Autowired
    private Resource systemMetadataResource3;
    @Autowired
    private Resource systemMetadataResource4;
    @Autowired
    private Resource systemMetadataResource5;

    @Test
    public void testSolrIndexRefreshTool() {

        // create a new SystemMetadata object for testing
        addSystemMetadata(systemMetadataResource1);
        addSystemMetadata(systemMetadataResource2);
        addSystemMetadata(systemMetadataResource3);
        addSystemMetadata(systemMetadataResource4);
        addSystemMetadata(systemMetadataResource5);

        try {
            SolrIndexBuildTool.main(null);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertFalse(true);
        }
        Assert.assertTrue(true);
    }

    @Before
    public void setUp() throws Exception {

        Config hzConfig = new ClasspathXmlConfig("org/dataone/configuration/hazelcast.xml");

        System.out.println("Hazelcast Group Config:\n" + hzConfig.getGroupConfig());
        System.out.print("Hazelcast Maps: ");
        for (String mapName : hzConfig.getMapConfigs().keySet()) {
            System.out.print(mapName + " ");
        }
        System.out.println();
        hzMember = Hazelcast.init(hzConfig);
        System.out.println("Hazelcast member hzMember name: " + hzMember.getName());

        sysMetaMap = hzMember.getMap(systemMetadataMapName);
        objectPaths = hzMember.getMap(objectPathName);
    }

    @After
    public void tearDown() throws Exception {
        Hazelcast.shutdownAll();
    }

    private void addSystemMetadata(Resource systemMetadataResource) {
        SystemMetadata sysmeta = null;
        try {
            sysmeta = TypeMarshaller.unmarshalTypeFromStream(SystemMetadata.class,
                    systemMetadataResource.getInputStream());
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            fail("Test SystemMetadata misconfiguration - Exception " + ex);
        }
        String path = null;
        try {
            path = StringUtils
                    .remove(systemMetadataResource.getFile().getPath(), "/SystemMetadata");
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        sysMetaMap.putAsync(sysmeta.getIdentifier(), sysmeta);
        objectPaths.putAsync(sysmeta.getIdentifier(), path);
    }

}
