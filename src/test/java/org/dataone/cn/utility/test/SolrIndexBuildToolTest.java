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

package org.dataone.cn.utility.test;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Set;

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

/**
 * This test currently relies upon: a postgres install and config see:
 * cn_index_common, and a solr server to connect to, see: solr.properties.
 * 
 * @author sroseboo
 * 
 */

@RunWith(SpringJUnit4ClassRunner.class)
// context files are located from the root of the test's classpath
// for example org/dataone/cn/index/test/
@ContextConfiguration(locations = { "test-context.xml" })
public class SolrIndexBuildToolTest {

    private static Logger logger = Logger.getLogger(SolrIndexBuildToolTest.class.getName());

    private static final String systemMetadataMapName = Settings.getConfiguration().getString(
            "dataone.hazelcast.systemMetadata");
    private static final String objectPathName = Settings.getConfiguration().getString(
            "dataone.hazelcast.objectPath");
    private static final String HZ_IDENTIFIERS = Settings.getConfiguration().getString(
            "dataone.hazelcast.identifiers");

    private HazelcastInstance hzMember;
    private IMap<Identifier, SystemMetadata> sysMetaMap;
    private IMap<Identifier, String> objectPaths;
    private Set<Identifier> pids;

    @Autowired
    private Resource systemMetadataResource1;
    @Autowired
    private Resource systemMetadataResource2;
    @Autowired
    private Resource systemMetadataResource3;
    @Autowired
    private Resource systemMetadataResource4;
    @Autowired
    private Resource systemMetadataResource5; // resource map for 1-4

    @Test
    public void testSolrIndexRefreshTool() {

        // create a new SystemMetadata object for testing
        addSystemMetadata(systemMetadataResource1);
        addSystemMetadata(systemMetadataResource2);
        addSystemMetadata(systemMetadataResource3);
        addSystemMetadata(systemMetadataResource4);
        addSystemMetadata(systemMetadataResource5);

        try {
            SolrIndexBuildTool.main(new String[] { "-a" });
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
        hzMember = Hazelcast.newHazelcastInstance(hzConfig);
        System.out.println("Hazelcast member hzMember name: " + hzMember.getName());

        sysMetaMap = hzMember.getMap(systemMetadataMapName);
        objectPaths = hzMember.getMap(objectPathName);
        pids = hzMember.getSet(HZ_IDENTIFIERS);
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
        pids.add(sysmeta.getIdentifier());
    }

}
