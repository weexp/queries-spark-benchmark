package com.stratio.deep.benchmark.cassandra.spark.datastaxt;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.hyperic.sigar.SigarException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.spark.connector.CassandraJavaUtil;
import com.datastax.spark.connector.CassandraRow;
import com.datastax.spark.connector.rdd.CassandraJavaRDD;
import com.stratio.deep.benchmark.cassandra.spark.filter.FunctionFilterDatastaxtPageCount;
import com.stratio.deep.benchmark.cassandra.spark.groupby.FunctionDatastaxtGroupByRev;
import com.stratio.deep.benchmark.cassandra.spark.groupby.FunctionDatastaxtMapRevGroupBy;
import com.stratio.deep.benchmark.cassandra.spark.join.FunctionDatastaxtMapRevJoin;
import com.stratio.deep.benchmark.cassandra.spark.join.FunctionMapDatastaxtPageJoin;

/**
 * Created by ParadigmaTecnologico on 22/05/2014.
 */

public class RunDataStaxtBench {

    private static final String pathTimes = "/home/stratio/logs/datastaxtconn/";
    private static final Logger logger = LoggerFactory
            .getLogger(RunDataStaxtBench.class);

    public static void main(String[] args) throws IOException, SigarException {
        final String CASSANDRAHOST = args[0];
        final String keyspace = args[1];
        final String table1 = args[2];
        final String table2 = args[3];
        final String splitSize = args[4];

        double time_start, time_end, tT;

        // context properties
        String cluster = "spark://" + args[0] + ":7077";
        String jobName = "stratioDeepExample";

        String pathJar = new File(RunDataStaxtBench.class.getProtectionDomain()
                .getCodeSource().getLocation().getPath()).getAbsolutePath();

        // String pathjar =
        // "C:\\Users\\ParadigmaTecnologico\\IdeaProjects\\bench\\target\\test-proyect-1.0-SNAPSHOT.jar";

        SparkConf sparkConf = new SparkConf();// .set("spark.executor.memory","2g");
        sparkConf.setMaster(cluster);
        sparkConf.setAppName(jobName);
        sparkConf.set("spark.executor.memory", "16g");
        sparkConf.set("spark.cassandra.input.split.size", splitSize);
        sparkConf.setJars(new String[] { pathJar });

        SparkContext sc = new SparkContext(sparkConf);
        try {
            CassandraJavaRDD<CassandraRow> rddRev = CassandraJavaUtil
                    .javaFunctions(sc)
                    .cassandraTable(keyspace, table1)
                    .select("id", "contributor_id", "contributor_isanonymous",
                            "contributor_username", "lucene", "page_fulltitle",
                            "page_id", "page_isredirect", "page_ns",
                            "page_restrictions", "page_title", "revision_id",
                            "revision_isminor", "revision_redirection",
                            "revision_timestamp");

            // Creating the RDD for PageCounts
            CassandraJavaRDD<CassandraRow> rddPage = CassandraJavaUtil
                    .javaFunctions(sc).cassandraTable(keyspace, table2);

            launchGroupByJob(rddRev);

            time_start = System.currentTimeMillis(); // Start Crono

            JavaRDD<CassandraRow> filtrado = rddPage
                    .filter(new FunctionFilterDatastaxtPageCount());

            System.out.println("\r\n Resultados Filter " + filtrado.count()
                    + "\r\n");

            time_end = System.currentTimeMillis(); // End crono
            tT = (time_end - time_start) / 1000;
            // data[i] = tT;
            // }

            File FileTimes_F = new File(pathTimes + "/Filter.txt");
            FileWriter TextOutTime_F = new FileWriter(FileTimes_F, true);
            TextOutTime_F
                    .write("RESPONSE TIME FILTER con tabla Revision y CON proyeccion: "
                            + tT + "\n");
            TextOutTime_F.close();

            time_start = System.currentTimeMillis(); // Start Crono

            JavaPairRDD<String, String> pairsRDDRev = rddRev
                    .mapToPair(new FunctionDatastaxtMapRevJoin());

            JavaPairRDD<String, Integer> pairsRDDPage = rddPage
                    .mapToPair(new FunctionMapDatastaxtPageJoin());

            long join = pairsRDDRev.join(pairsRDDPage).count();

            time_end = System.currentTimeMillis(); // End crono
            tT = (time_end - time_start) / 1000;

            System.out.println("Count Join: " + join);

            File FileTimes_J = new File(pathTimes + "/Join.txt");
            FileWriter TextOutTime_J = new FileWriter(FileTimes_J, true);
            TextOutTime_J.write("RESPONSE TIME JOIN: " + tT + "\n");
            TextOutTime_J.close();

        } finally {
            sc.stop();
        }

        System.exit(0);
    }

    private static void launchGroupByJob(CassandraJavaRDD<CassandraRow> rddRev)
            throws IOException {

        long time_start = System.currentTimeMillis(); // Start Crono
        JavaPairRDD<String, Iterable<CassandraRow>> groups = rddRev
                .groupBy(new FunctionDatastaxtGroupByRev());
        JavaPairRDD<String, Integer> counts = groups
                .mapToPair(new FunctionDatastaxtMapRevGroupBy());

        long results = counts.count();

        long time_end = System.currentTimeMillis(); // End crono
        long tT = (time_end - time_start) / 1000;

        System.out.println("Count GroupBy: " + results);

        File FileTimes_G = new File(pathTimes + "/Group.txt");
        FileWriter TextOutTime_G = new FileWriter(FileTimes_G, true);
        TextOutTime_G.write("RESPONSE TIME GROUPBY: " + tT + "\n");
        TextOutTime_G.close();

    }
}
