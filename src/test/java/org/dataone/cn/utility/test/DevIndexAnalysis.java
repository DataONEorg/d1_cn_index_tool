package org.dataone.cn.utility.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.SolrParams;
import org.dataone.client.auth.CertificateManager;
import org.dataone.client.v2.CNode;
import org.dataone.client.v2.itk.D1Client;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.DescribeResponse;
import org.dataone.service.types.v1.ObjectInfo;
import org.dataone.service.types.v1.TypeFactory;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DevIndexAnalysis {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
           
    }

    @Before
    public void setUp() throws Exception {
 //       System.setProperty("D1Client.CN_URL","https://cn-dev.test.dataone.org/cn");
    }

  //  @Test
    public void test() {
        fail("Not yet implemented"); // TODO
    }
    
    @Test
    public void testCompareIndexToCNRepo() throws IOException, ServiceFailure, NotImplemented, SolrServerException {
        
        CertificateManager.getInstance().setCertificateLocation("/etc/dataone/client/testClientCerts/cnDevUNM1.crt");
        Settings.getConfiguration().setProperty("D1Client.CN_URL","https://cn-dev.test.dataone.org/cn");
        CNode cn = D1Client.getCN();
        // get all Index records
        int start = 0;
        while (true) {
     
            SolrClient sc = new HttpSolrClient(D1Client.getCN().getNodeBaseServiceUrl() + "/query/solr");

            SolrQuery sp = new SolrQuery();
            sp.set("q", "*:*");
//            sp.set("wt", "javabin");
            sp.set("fl", "id,authoritativeMN,formatId");
            sp.set("rows", 1000);
            sp.set("start", start);
            sp.setRequestHandler("/");
            QueryResponse res = sc.query(sp);
  
            SolrDocumentList list = res.getResults();
            if ( list.size() == 0) {
                break;
            }
            System.out.println("Query Start " + start + " returned " + list.size());
//            for (SolrDocument doc :  list) {
//                System.out.print(".");
//                String pid = (String) doc.getFieldValue("id");
//                try {
//                   DescribeResponse dr = cn.describe(null, TypeFactory.buildIdentifier(pid));
//                   
//                } catch (InvalidToken | NotAuthorized e) {
//                    // TODO Auto-generated catch block
//                    e.printStackTrace();
//                } catch (NotFound e) {
//                    System.out.println("NotFound : " + pid);
//                }
//            }
            
            start += 1000;
            System.out.println("Next start = " + start);
            
        }
    }

}
