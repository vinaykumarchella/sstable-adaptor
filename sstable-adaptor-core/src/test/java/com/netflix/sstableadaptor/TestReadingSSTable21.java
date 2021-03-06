package com.netflix.sstableadaptor;

import com.netflix.sstableadaptor.sstable.SSTableIterator;
import com.netflix.sstableadaptor.sstable.SSTableSingleReader;
import com.netflix.sstableadaptor.util.SSTableUtils;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CFProperties;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.util.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * These tests are for reading SSTable in Cassandra 2.1.
 * Note that the code would probably work for Cassandra 2.2 files too but we don't test them here
 */
public class TestReadingSSTable21 extends TestBaseSSTableFunSuite {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestReadingSSTable21.class);

    /**
     * Setting up resources prior to running any tests.
     * @throws Exception when we cannot initialize the resources
     */
    @BeforeClass
    public static void setup() throws Exception {
        LOGGER.info("Running TestReadingSSTable21 setup ...");
        TestBaseSSTableFunSuite.setup();
    }

    /**
     * Tear down resources after all tests.
     * @throws Exception when teardown has an issue
     */
    @AfterClass
    public static void teardown() throws Exception {
        LOGGER.info("Tearing TestReadingSSTable21 down ...");
        TestBaseSSTableFunSuite.teardown();
    }

    /**
     *  Test on a local C* 2.1 data
     *      compressed_bills-03c5a7b0643c11e7936273d8df3aeac7/casspactor2-compressed_bills-ka-1-Data.db
     *  with composite partition key.
     */
    @Test
    public void testOnLocalDataCompositePartitionKey() throws IOException {
        final String cql = "CREATE TABLE casspactor2.compressed_bills (\n" +
                "    user text," +
                "    email text," +
                "    expense_id int," +
                "    item_id int," +
                "    account_id text static," +
                "    amount int," +
                "    balance int static," +
                "    name text," +
                "    PRIMARY KEY ((user, email), expense_id, item_id)" +
                ") WITH CLUSTERING ORDER BY (expense_id ASC, item_id ASC)" +
                "    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}" +
                "    AND compression = {'chunk_length_in_kb': '64', " +
                "                       'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}";


        final String inputSSTableFullPathFileName = CASS21_DATA_DIR +
                        "keyspace1/compressed_bills-03c5a7b0643c11e7936273d8df3aeac7/casspactor2-compressed_bills-ka-1-Data.db";

        final int counter = getRowCount(inputSSTableFullPathFileName, cql, false);

        LOGGER.info("\nCounter: " + counter);
        Assert.assertEquals(15, counter);
    }

    /**
     *  Test on a local C* 2.1 data
     *      compressed_bills-03c5a7b0643c11e7936273d8df3aeac7/casspactor2-compressed_bills-ka-1-Data.db
     *  with composite partition key.
     */
    @Test
    public void testOnLocalDataCompositePartitionKey2() throws IOException {
        final String cql = "CREATE TABLE casspactor2.viewing_history (" +
                "    user text PRIMARY KEY," +
                "    movie_id text" +
                ") WITH compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}" +
                "    AND compression = {'chunk_length_in_kb': '64', " +
                "                       'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}";

        final String inputSSTableFullPathFileName = CASS21_DATA_DIR + "keyspace1/" +
                            "viewing_history-04d2b570643d11e7936273d8df3aeac7/casspactor2-viewing_history-ka-1-Data.db";

        final int counter = getRowCount(inputSSTableFullPathFileName, cql, false);

        LOGGER.info("\nCounter: " + counter);
        Assert.assertEquals(9, counter);
    }


    /**
     *  Test on a local C* 2.1 data
     *      compressed_bills-03c5a7b0643c11e7936273d8df3aeac7/casspactor2-compressed_bills-ka-1-Data.db
     *  with composite partition key.
     */
    @Test
    public void testSubscriberData() throws IOException {
        final String cql = "CREATE TABLE casspactor2.\"CustomerData\" (\n" +
                "    \"KEY\" text,\n" +
                "    column1 text,\n" +
                "    value blob,\n" +
                "    PRIMARY KEY (\"KEY\", column1)\n" +
                ") WITH COMPACT STORAGE\n" +
                "    AND CLUSTERING ORDER BY (column1 ASC)\n" +
                "    AND comment = 'Customer Data Records'\n" +
                "    AND compaction = {'min_threshold': '2', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}\n" +
                "    AND compression = {};";

        final String inputSSTableFullPathFileName = CASS21_DATA_DIR +
                "CustomerData/Membership-CustomerData-ka-215474-Data.db";

        final int counter = getRowCount(inputSSTableFullPathFileName, cql, false);

        LOGGER.info("\nCounter: " + counter);
        Assert.assertEquals(9, counter);
    }

    @Test
    public void testWriteSubscriberData() throws IOException {
        final String cql = "CREATE TABLE casspactor2.\"CustomerData\" (\n" +
                "    \"KEY\" text,\n" +
                "    column1 text,\n" +
                "    value blob,\n" +
                "    PRIMARY KEY (\"KEY\", column1)\n" +
                ") WITH COMPACT STORAGE\n" +
                "    AND CLUSTERING ORDER BY (column1 ASC)\n" +
                "    AND comment = 'Customer Data Records'\n" +
                "    AND compaction = {'min_threshold': '2', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}\n" +
                "    AND compression = {};";

        final String inputSSTableFullPathFileName = CASS21_DATA_DIR +
                "CustomerData/Membership-CustomerData-ka-215474-Data.db";

        LOGGER.info("Input file name: " + inputSSTableFullPathFileName);
        CFMetaData cfMetaData = CFMetaData.compile(cql, "casspactor2", "RandomPartitioner");

        final SSTableSingleReader sstableSingleReader =
                new SSTableSingleReader(inputSSTableFullPathFileName, cfMetaData, TestBaseSSTableFunSuite.HADOOP_CONF);
//        final ISSTableScanner currentScanner =
//                sstableSingleReader.getSSTableScanner(Long.MIN_VALUE, Long.MAX_VALUE);

        final ISSTableScanner currentScanner =
                sstableSingleReader.getSSTableScanner(BigInteger.ZERO, new BigInteger("2").pow(127),null);





        ////WRITE

        LOGGER.info("Input file name: " + inputSSTableFullPathFileName);

        final Descriptor inputSSTableDescriptor = Descriptor.fromFilename(inputSSTableFullPathFileName,
                TestBaseSSTableFunSuite.HADOOP_CONF);
        SSTableWriter writer = null;

//            SSTableSingleReader reader = new SSTableSingleReader(inputSSTableFullPathFileName,
//                    "casspactor",
//                    "bills_nc",
//                    TestBaseSSTableFunSuite.HADOOP_CONF);
            final CFMetaData inputCFMetaData = sstableSingleReader.getCfMetaData();
//            final ISSTableScanner currentScanner = reader.getSSTableScanner();
            final SSTableReader inputSStable = sstableSingleReader.getSstableReader();

            //Create writer
            final CFMetaData outputCFMetaData = SSTableUtils.createNewCFMetaData(inputSSTableDescriptor, inputCFMetaData);
            writer = SSTableUtils.createSSTableWriter(inputSSTableDescriptor, outputCFMetaData, inputSStable);

            while (currentScanner.hasNext()) {
                final UnfilteredRowIterator row = currentScanner.next();
                while (row.hasNext()) {

                    final Row uRow = (Row) row.next();
                    Iterator<Cell> cellsIterator = uRow.cells().iterator();
                    while (cellsIterator.hasNext()) {
                        Cell cell = cellsIterator.next();
                        LOGGER.info("Type: " + cell.column().type);
                        LOGGER.info("Column Name: " + cell.column().name);
                        LOGGER.info("\t\t" + cell.toString());
                        cell = cell.withUpdatedValue(ByteBuffer.wrap("Vinay".getBytes()));

                        LOGGER.info("\t After Update" + cell.toString());
                    }
                }

                writer.append(row);
            }
            writer.finish(false);


    }


    /**
     *  Test on a local C* 2.1 data
     *      user_profiles-b355bee0669911e7a49e993faaf28cc4/abc-user_profiles-ka-1-Data.db
     *  with composite partition key.
     */
    @Test
    public void testOnLocalDataThriftCreatedTable() throws IOException {
        final String cql = "CREATE TABLE abc.user_profiles (" +
                "    key text PRIMARY KEY," +
                "    email text," +
                "    first_name text," +
                "    last_name text," +
                "    year_of_birth varint" +
                ") WITH COMPACT STORAGE" +
                "    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', " +
                "                      'max_threshold': '32', 'min_threshold': '4'}" +
                "    AND compression = {'chunk_length_in_kb': '64', " +
                "                       'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}";

        final String inputSSTableFullPathFileName = CASS21_DATA_DIR + "keyspace1/" +
                                       "user_profiles-b355bee0669911e7a49e993faaf28cc4/abc-user_profiles-ka-1-Data.db";

        final int counter = getRowCount(inputSSTableFullPathFileName, cql, true);

        LOGGER.info("\nCounter: " + counter);
        Assert.assertEquals(3, counter);
    }

    private int getRowCount(final String inputSSTableFullPathFileName, String cql,
                            boolean isThriftTable) throws IOException {
        LOGGER.info("Input file name: " + inputSSTableFullPathFileName);
        int counter = 0;
        CFMetaData cfMetaData = CFMetaData.compile(cql, "casspactor2", "RandomPartitioner");
        final SSTableSingleReader sstableSingleReader =
                new SSTableSingleReader(inputSSTableFullPathFileName, cfMetaData, TestBaseSSTableFunSuite.HADOOP_CONF);
//        final ISSTableScanner currentScanner =
//                sstableSingleReader.getSSTableScanner(Long.MIN_VALUE, Long.MAX_VALUE);

        final ISSTableScanner currentScanner =
                sstableSingleReader.getSSTableScanner(BigInteger.ZERO, new BigInteger("2").pow(127),null);


        final int nowInSecs = (int) (System.currentTimeMillis() / 1000);
        final List<ISSTableScanner> scanners = new ArrayList<>();
        scanners.add(currentScanner);
        try (SSTableIterator ci = new SSTableIterator(scanners, cfMetaData, nowInSecs)) {
            while (ci.hasNext()) {
                final RowIterator rowIterator = ci.next();
                counter += printRowDetails(cfMetaData, rowIterator, isThriftTable);
            }
        }

        return counter;
    }

    @Test
    public void testParsingCQLOnCompoundedTable() {
       String cql = "CREATE TABLE keyspace1.compressed_bills (" +
               "     user text," +
               "     email text," +
               "     account_id text static," +
               "     balance int static," +
               "     expense_id int," +
               "     item_id int," +
               "     amount int," +
               "     name text," +
               "     PRIMARY KEY ((user, email), expense_id, item_id))";


        ParsedStatement stmt = QueryProcessor.parseStatement(cql);
        stmt.properties.properties.addProperty(CFProperties.KEYSPACE_NAME, "keyspace1");
        stmt.properties.properties.addProperty(CFProperties.PARTITIONER_CLASS,
                                               "org.apache.cassandra.dht.Murmur3Partitioner");

        ParsedStatement.Prepared preparedStmt = stmt.prepare();

        CFMetaData cfMetaData = ((CreateTableStatement) preparedStmt.statement).getCFMetaData();
        Assert.assertEquals(cfMetaData.ksName, "keyspace1");
        Assert.assertEquals(cfMetaData.partitioner, Murmur3Partitioner.instance);

    }

    @Test
    public void testParsingCQLOnCompoundedTableWithCFMetaData() {
        String cql = "CREATE TABLE compressed_bills (" +
                "     user text," +
                "     email text," +
                "     account_id text static," +
                "     balance int static," +
                "     expense_id int," +
                "     item_id int," +
                "     amount int," +
                "     name text," +
                "     PRIMARY KEY ((user, email), expense_id, item_id))";

        CFMetaData cfMetaData = CFMetaData.compile(cql, "keyspace1");

        Assert.assertEquals(cfMetaData.ksName, "keyspace1");
        Assert.assertEquals(cfMetaData.cfName, "compressed_bills");


        cfMetaData.primaryKeyColumns().forEach(col -> {
            String colName = col.name.toString();
            Assert.assertTrue(colName.equals("user") || colName.equals("email") ||
                              colName.equals("expense_id") || colName.equals("item_id"));
        });

        cfMetaData.partitionColumns().forEach(col -> {
            String colName = col.name.toString();
            Assert.assertTrue(colName.equals("account_id") || colName.equals("balance") ||
                              colName.equals("amount") || colName.equals("name"));
        });

        cfMetaData.clusteringColumns().forEach(col -> {
            String colName = col.name.toString();
            Assert.assertTrue(colName.equals("expense_id") || colName.equals("item_id"));
        });

        Assert.assertTrue(8 == cfMetaData.allColumns().size());

        Assert.assertFalse(cfMetaData.isStaticCompactTable());
        Assert.assertTrue(cfMetaData.isCompound());
        Assert.assertTrue(cfMetaData.hasStaticColumns());
        Assert.assertTrue(cfMetaData.isCQLTable());
        Assert.assertFalse(cfMetaData.isView());
        Assert.assertFalse(cfMetaData.isCompactTable());
        Assert.assertFalse(cfMetaData.isCounter());
        Assert.assertFalse(cfMetaData.isDense());
        Assert.assertFalse(cfMetaData.isIndex());
        Assert.assertFalse(cfMetaData.isSuper());
        Assert.assertFalse(cfMetaData.isThriftCompatible());
        Assert.assertFalse(cfMetaData.hasCollectionColumns());
        Assert.assertFalse(cfMetaData.hasComplexColumns());

        Assert.assertEquals(cfMetaData.partitioner, Murmur3Partitioner.instance);
    }

    @Test
    public void testParsingCQLOnSimpleTable() throws IOException {
        final String cql = "CREATE TABLE casspactor2.viewing_history (" +
                "    user text PRIMARY KEY," +
                "    movie_id text" +
                ") WITH compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', " +
                "                      'max_threshold': '32', 'min_threshold': '4'}" +
                "    AND compression = {'chunk_length_in_kb': '64', " +
                "                       'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}";

        CFMetaData cfMetaData = CFMetaData.compile(cql, "casspactor2");

        Assert.assertEquals(cfMetaData.ksName, "casspactor2");
        Assert.assertEquals(cfMetaData.cfName, "viewing_history");

        cfMetaData.primaryKeyColumns().forEach(col -> {
            String colName = col.name.toString();
            Assert.assertTrue(colName.equals("user"));
        });

        cfMetaData.partitionColumns().forEach(col -> {
            String colName = col.name.toString();
            Assert.assertTrue(colName.equals("movie_id"));
        });

        Assert.assertTrue(cfMetaData.clusteringColumns().isEmpty());
        Assert.assertTrue(2 == cfMetaData.allColumns().size());
        Assert.assertFalse(cfMetaData.isStaticCompactTable());
        Assert.assertTrue(cfMetaData.isCompound());
        Assert.assertFalse(cfMetaData.hasStaticColumns());
        Assert.assertTrue(cfMetaData.isCQLTable());
        Assert.assertFalse(cfMetaData.isView());
        Assert.assertFalse(cfMetaData.isCompactTable());
        Assert.assertFalse(cfMetaData.isCounter());
        Assert.assertFalse(cfMetaData.isDense());
        Assert.assertFalse(cfMetaData.isIndex());
        Assert.assertFalse(cfMetaData.isSuper());
        Assert.assertFalse(cfMetaData.isThriftCompatible());
        Assert.assertFalse(cfMetaData.hasCollectionColumns());
        Assert.assertFalse(cfMetaData.hasComplexColumns());

        Assert.assertEquals(cfMetaData.partitioner, Murmur3Partitioner.instance);
    }

    @Test
    public void testParsingCQLOnThriftCreatedTable() throws IOException {
        final String cql = "CREATE TABLE abc.user_profiles (" +
                "    key text PRIMARY KEY," +
                "    email text," +
                "    first_name text," +
                "    last_name text," +
                "    year_of_birth varint" +
                ") WITH COMPACT STORAGE" +
                "    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}" +
                "    AND compression = {'chunk_length_in_kb': '64', " +
                "                       'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}";

        CFMetaData cfMetaData = CFMetaData.compile(cql, "abc");

        Assert.assertEquals(cfMetaData.ksName, "abc");
        Assert.assertEquals(cfMetaData.cfName, "user_profiles");

        cfMetaData.primaryKeyColumns().forEach(col -> {
            String colName = col.name.toString();
            Assert.assertTrue(colName.equals("key") || colName.equals("column1"));
        });

        cfMetaData.partitionColumns().forEach(col -> {
            String colName = col.name.toString();
            Assert.assertTrue(colName.equals("email") ||
                              colName.equals("first_name") ||
                              colName.equals("last_name") ||
                              colName.equals("year_of_birth") ||
                              colName.equals("value"));
        });

        cfMetaData.clusteringColumns().forEach(col -> {
            String colName = col.name.toString();
            Assert.assertTrue(colName.equals("column1"));
        });

        Assert.assertFalse(cfMetaData.clusteringColumns().isEmpty());
        Assert.assertTrue(7 == cfMetaData.allColumns().size());
        Assert.assertTrue(cfMetaData.isStaticCompactTable());
        Assert.assertFalse(cfMetaData.isCompound());
        Assert.assertTrue(cfMetaData.hasStaticColumns());
        Assert.assertFalse(cfMetaData.isCQLTable());
        Assert.assertFalse(cfMetaData.isView());
        Assert.assertTrue(cfMetaData.isCompactTable());
        Assert.assertFalse(cfMetaData.isCounter());
        Assert.assertFalse(cfMetaData.isDense());
        Assert.assertFalse(cfMetaData.isIndex());
        Assert.assertFalse(cfMetaData.isSuper());
        Assert.assertTrue(cfMetaData.isThriftCompatible());
        Assert.assertFalse(cfMetaData.hasCollectionColumns());
        Assert.assertFalse(cfMetaData.hasComplexColumns());

        Assert.assertEquals(cfMetaData.partitioner, Murmur3Partitioner.instance);
    }

    @Test
    public void testParsingCQLOnUnstructuredThriftCreateTable() throws IOException {
        final String cql = "CREATE TABLE cptests.ab_enums (" +
                "key text,  " +
                "column1 text,   " +
                "value blob, " +
                "PRIMARY KEY (key, column1)) " +
                "WITH COMPACT STORAGE AND CLUSTERING ORDER BY (column1 ASC) " +
                "AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}";

        CFMetaData cfMetaData = CFMetaData.compile(cql, "cptests");

        Assert.assertEquals(cfMetaData.ksName, "cptests");
        Assert.assertEquals(cfMetaData.cfName, "ab_enums");

        cfMetaData.primaryKeyColumns().forEach(col -> {
            String colName = col.name.toString();
            Assert.assertTrue(colName.equals("key") || colName.equals("column1"));
        });

        cfMetaData.partitionColumns().forEach(col -> {
            Assert.assertEquals("value", col.name.toString());
        });

        cfMetaData.clusteringColumns().forEach(col -> {
            String colName = col.name.toString();
            Assert.assertTrue(colName.equals("column1"));
        });

        Assert.assertFalse(cfMetaData.clusteringColumns().isEmpty());
        Assert.assertTrue(3 == cfMetaData.allColumns().size());
        Assert.assertTrue(cfMetaData.isCompactTable());
        Assert.assertFalse(cfMetaData.isCompound());
        Assert.assertFalse(cfMetaData.hasStaticColumns());
        Assert.assertFalse(cfMetaData.isCQLTable());
        Assert.assertFalse(cfMetaData.isView());
        Assert.assertTrue(cfMetaData.isCompactTable());
        Assert.assertFalse(cfMetaData.isCounter());
        Assert.assertTrue(cfMetaData.isDense());
        Assert.assertFalse(cfMetaData.isIndex());
        Assert.assertFalse(cfMetaData.isSuper());
        Assert.assertTrue(cfMetaData.isThriftCompatible());
        Assert.assertFalse(cfMetaData.hasCollectionColumns());
        Assert.assertFalse(cfMetaData.hasComplexColumns());

        Assert.assertEquals(cfMetaData.partitioner, Murmur3Partitioner.instance);
    }

}


