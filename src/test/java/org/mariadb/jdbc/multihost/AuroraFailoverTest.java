package org.mariadb.jdbc.multihost;

import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClient;
import com.amazonaws.services.rds.model.*;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AuroraFailoverTest extends BaseMultiHostTest {

    /*@Test
    public void test() throws SQLException {
        Assume.assumeTrue(auroraUrlOk);
        AmazonRDS dd = new AmazonRDSClient();

        DescribeDBInstancesResult instances  =dd.describeDBInstances();

        for (DBInstance instance : instances.getDBInstances()) {
            System.out.println(" --------------------------------------------");
            System.out.println(" instance DBNAME : " + instance.getDBName());
            System.out.println(" instance DBInstanceIdentifier : " + instance.getDBInstanceIdentifier());
            System.out.println(" instance DBInstanceStatus : " + instance.getDBInstanceStatus());
            System.out.println(" instance MasterUsername : " + instance.getMasterUsername());
            System.out.println(" instance MultiAZ : " + instance.getMultiAZ());
            System.out.println(" instance getStorageType : " + instance.getStorageType());

            for (String instanceRead : instance.getReadReplicaDBInstanceIdentifiers()) {
                System.out.println(" --- read replica DBInstanceIdentifier : " + instanceRead);
            }
            System.out.println(" instance ReadReplicaSourceDBInstanceIdentifier : " + instance.getReadReplicaSourceDBInstanceIdentifier());
        }

        RebootDBInstanceRequest rebootRequest = new RebootDBInstanceRequest();
        rebootRequest.setDBInstanceIdentifier("mariadb-aurora-1");
        rebootRequest.withForceFailover(true);
        dd.rebootDBInstance(rebootRequest);

        PromoteReadReplicaRequest promoteRequest = new PromoteReadReplicaRequest();
        promoteRequest.setDBInstanceIdentifier("mariadb-aurora-2");
        dd.promoteReadReplica(promoteRequest);
    }*/

    /*@Test
    public void testMultiHostWriteOnMaster() throws SQLException {
        Assume.assumeTrue(auroraUrlOk);
        Connection connection = null;
        try {
            connection = getAuroraNewConnection();
            Assert.assertFalse(connection.isReadOnly());
            connection.setReadOnly(true);
            Assert.assertTrue(connection.isReadOnly());
        } finally {
            if(connection!=null) connection.close();
        }
    }*/

    @Test
    public void testManualFailover() throws SQLException, InterruptedException {
        Assume.assumeTrue(auroraUrlOk);
        Connection connection = null;
        try {
            connection = getAuroraNewConnection("&secondsBeforeRetryMaster=2");
            Assert.assertFalse(connection.isReadOnly());
            long beginTest = System.currentTimeMillis();
            boolean waitToSwitch = true;

            while(waitToSwitch) {
                Thread.sleep(1000);
                try {
                    waitToSwitch = !connection.isReadOnly();
                }catch (SQLException e) {e.printStackTrace();}
            }
            log.fine("failover automatically done after "+((System.currentTimeMillis() - beginTest)));
            boolean waitTobackMaster = true;
            while(waitTobackMaster) {
                Thread.sleep(1000);
                try {
                    waitTobackMaster = connection.isReadOnly();
                }catch (SQLException e) {e.printStackTrace();}
            }
            log.fine("return on master automatically done after "+((System.currentTimeMillis() - beginTest)));


        } finally {
            if(connection!=null) connection.close();
        }
    }
}
