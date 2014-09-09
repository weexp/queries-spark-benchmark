package com.stratio.deep.benchmark.cassandra.spark;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.hyperic.sigar.SigarException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.deep.benchmark.cassandra.spark.filter.FunctionFilterPageCount;
import com.stratio.deep.benchmark.cassandra.spark.groupby.FunctionGroupByRev;
import com.stratio.deep.benchmark.cassandra.spark.groupby.FunctionMapRevGroupBy;
import com.stratio.deep.benchmark.cassandra.spark.join.FunctionMapPageJoin;
import com.stratio.deep.benchmark.cassandra.spark.join.FunctionMapRevJoin;
import com.stratio.deep.config.CassandraConfigFactory;
import com.stratio.deep.config.ICassandraDeepJobConfig;
import com.stratio.deep.context.CassandraDeepSparkContext;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.rdd.CassandraJavaRDD;

/**
 * Created by ParadigmaTecnologico on 22/05/2014.
 */

public class RunBench {

    private static final Logger logger = LoggerFactory
            .getLogger(RunBench.class);

    public static void main(String[] args) throws IOException, SigarException {
        final String CASSANDRAHOST = args[0];
        final String pathTime = args[1];
        final String pathFileF = args[2];
        final String pathFileG = args[3];
        final String pathFileJ = args[4];
        final String keyspace = args[5];
        final String table1 = args[6];
        final String table2 = args[7];
        final Integer splitSize = Integer.valueOf(args[8]);
        final Integer pageSize = Integer.valueOf(args[9]);

        final List<String> slaves = Arrays.asList(args).subList(9, args.length);

        List<FileFilter> filterList = new ArrayList<>();

        double time_start, time_end, tT;
        String max, min, avg;

        // context properties
        String cluster = "spark://" + args[0] + ":7077";
        String jobName = "stratioDeepExample";
        String pathF = pathTime + "/filterTime.txt";
        String pathG = pathTime + "/groupTime.txt";
        String pathJ = pathTime + "/joinTime.txt";
        int cassandraPort = 9160;

        String pathJar = new File(RunBench.class.getProtectionDomain()
                .getCodeSource().getLocation().getPath()).getAbsolutePath();

        // String pathjar =
        // "C:\\Users\\ParadigmaTecnologico\\IdeaProjects\\bench\\target\\test-proyect-1.0-SNAPSHOT.jar";

        SparkConf sparkConf = new SparkConf();// .set("spark.executor.memory","2g");
        sparkConf.setMaster(cluster);
        sparkConf.setAppName(jobName);
        sparkConf.set("spark.executor.memory", "16g");
        sparkConf.setJars(new String[] { pathJar });

        SparkContext sc = new SparkContext(sparkConf);

        // Creating Context
        CassandraDeepSparkContext deepContext = new CassandraDeepSparkContext(
                sc);
        try {
            // Configuration and initialization for Revision
            // initialization files Master for Filter
            // FileFilter fileFilter_M = new FileFilter(CASSANDRAHOST,
            // pathFileF);
            // fileFilter_M.start();
            // initialization files Slaves for Filter
            // for (int i = 9; i == args.length; i++) {
            // FileFilter fileFilter_S = new FileFilter(slaves.get(i),
            // pathFileF);
            // fileFilter_S.start();
            // }

            // Configuration and initialization for PageCounts
            ICassandraDeepJobConfig<Cells> configPage = CassandraConfigFactory
                    .create().host(CASSANDRAHOST).rpcPort(cassandraPort)
                    .keyspace(keyspace).table(table2).splitSize(splitSize)
                    .initialize().pageSize(pageSize);

            // Creating the RDD for PageCounts
            CassandraJavaRDD<Cells> rddPage = deepContext
                    .cassandraJavaRDD(configPage);

            ICassandraDeepJobConfig<Cells> configRev = CassandraConfigFactory
                    .create()
                    .host(CASSANDRAHOST)
                    .rpcPort(cassandraPort)
                    .keyspace(keyspace)
                    .table(table1)
                    .inputColumns("id", "contributor_id",
                            "contributor_isanonymous", "contributor_username",
                            "lucene", "page_fulltitle", "page_id",
                            "page_isredirect", "page_ns", "page_restrictions",
                            "page_title", "revision_id", "revision_isminor",
                            "revision_redirection", "revision_timestamp")
                    .splitSize(splitSize).initialize().pageSize(pageSize);

            // Creating the RDD for Revision
            CassandraJavaRDD<Cells> rddRev = deepContext
                    .cassandraJavaRDD(configRev);

            // FILTER
            // for (int i = 0; i < runs; i++) {

            time_start = System.currentTimeMillis(); // Start Crono

            // Function Filter: FunctionFilterPageCount -> count pages in a
            // limit times
            JavaRDD<Cells> filtrado = rddPage
                    .filter(new FunctionFilterPageCount());
            long resultados = filtrado.count();

            System.out.println("\r\n Resultados Filter con pagesize 10.000 "
                    + resultados + "\r\n");
            // Configuration and initialization for PageCounts with secondary
            // index
            time_end = System.currentTimeMillis(); // End crono
            tT = (time_end - time_start) / 1000d;

            long timeS2IStart = System.currentTimeMillis();
            ICassandraDeepJobConfig<Cells> configPageWithFilter = CassandraConfigFactory
                    .create()
                    .rpcPort(cassandraPort)
                    .keyspace(keyspace)
                    .table(table2)
                    .splitSize(splitSize)
                    .filterByField(
                            "lucene",
                            // "{filter : {type : \"range\",field : \"page_id\", lower : 18295009 , include_lower : true , upper : 18295020 , include_upper : false }}")
                            "{filter : {type : \"range\",field : \"pagecounts\", lower : 199 , include_lower : true , upper : 201 , include_upper : false }}")
                    .pageSize(pageSize).initialize();

            // Creating the RDD for PageCounts with secondary index

            CassandraJavaRDD<Cells> rddPageWithFilter = deepContext
                    .cassandraJavaRDD(configPageWithFilter);
            long resultadosIndex = rddPageWithFilter.count();
            System.out.println("Secondary Index Filter " + resultadosIndex);
            long timeS2IStop = System.currentTimeMillis();
            // data[i] = tT;
            // }

            File FileTimes_F = new File(pathF);
            FileWriter TextOutTime_F = new FileWriter(FileTimes_F, true);
            TextOutTime_F.write("RESPONSE TIME FILTER con " + splitSize + ": "
                    + tT + "\n");
            TextOutTime_F.write("RESPONSE TIME FILTER WITH 2I con " + splitSize
                    + ": " + (timeS2IStop - timeS2IStart) / 1000d + "\n");
            TextOutTime_F.close();

            /**/

            launchGroupByJob(rddRev, slaves, CASSANDRAHOST, pathFileG, pathG,
                    splitSize);

            // Calculate max
            // Bench benchMaxF = new Bench();
            // String.format("%.2f", benchMaxF.max(data, runs));

            // Calculate min
            // Bench benchMinF = new Bench();
            // String.format("%.2f", benchMinF.min(data, runs));

            // Calculate avg
            // Bench benchAvgF = new Bench();
            // String.format("%.2f", benchAvgF.media(data, runs));

            // stop file Master for Filter

            // fileFilter_M.stop();

            // stop files Slaves for Filter

            // for (int i = 9; i == args.length; i++) {
            // FileFilter fileFilter_S = new FileFilter(slaves.get(i),
            // pathFileF);
            // fileFilter_S.stop();
            // }

            // initialization files Master for Join

            // FileJoin fileJoin_M = new FileJoin(args[0], pathFileJ);
            // fileJoin_M.start();

            // initialization files Slaves for Join

            // for (int i = 9; i == args.length; i++) {
            // FileJoin fileJoin_S = new FileJoin(slaves.get(i), pathFileJ);
            // fileJoin_S.start();
            // }

            // JOIN
            // for (int i = 0; i < runs; i++) {

            time_start = System.currentTimeMillis(); // Start Crono

            // Function Join
            // RDD Revision
            JavaPairRDD<String, String> pairsRDDRev = rddRev
                    .mapToPair(new FunctionMapRevJoin());
            // System.out.println("\r\n Resultados Map Tabla Revision "+
            // pairsRDDRev.collect() + "\r\n");

            // RDD PageCounts
            JavaPairRDD<String, Integer> pairsRDDPage = rddPage
                    .mapToPair(new FunctionMapPageJoin());
            // System.out.println("\r\n Resultados Map Tabla PageCounts "+
            // pairsRDDPage.collect() + "\r\n");

            // Join
            long join = pairsRDDRev.join(pairsRDDPage).count();
            // List<Tuple2<String, Tuple2<String, Integer>>> join =
            // pairsRDDRev.join(pairsRDDPage).collect();
            // System.out.println("\r\n Resultados Join por campo titulo " +
            // join+
            // "\r\n");

            time_end = System.currentTimeMillis(); // End crono
            tT = (time_end - time_start) / pageSize;
            // data[i] = tT;
            // }

            System.out.println("Count Join: " + join);

            File FileTimes_J = new File(pathJ);
            FileWriter TextOutTime_J = new FileWriter(FileTimes_J, true);
            TextOutTime_J.write("RESPONSE TIME JOIN: " + splitSize + ": " + tT
                    + "\n");
            TextOutTime_J.close();

            // Calculate max
            // Bench benchMaxJ = new Bench();
            // String.format("%.2f", benchMaxJ.max(data, runs));

            // Calculate min
            // Bench benchMinJ = new Bench();
            // String.format("%.2f", benchMinJ.min(data, runs));

            // Calculate avg
            // Bench benchAvgJ = new Bench();
            // String.format("%.2f", benchAvgJ.media(data, runs));

            // stop files Master for Join

            // stop file Master for Join

            // fileJoin_M.stop();

            // stop files Salve for Join

            // for (int i = 9; i == args.length; i++) {
            // FileJoin fileJoin_S = new FileJoin(slaves.get(i), pathFileJ);
            // fileJoin_S.stop();
            // }

        } finally {
            deepContext.stop();
        }

        System.exit(0);
    }

    private static void launchGroupByJob(CassandraJavaRDD<Cells> rddRev,
            List<String> slaves, String CASSANDRAHOST, String pathFileG,
            String pathG, int bisecFactor) throws IOException {

        // initialization files Master for Group
        // FileGroup fileGroup_M = new FileGroup(CASSANDRAHOST, pathFileG);
        // fileGroup_M.start();
        // // initialization files Slaves for Group
        // for (int i = 0; i < slaves.size(); i++) {
        // FileGroup fileGroup_S = new FileGroup(slaves.get(i), pathFileG);
        // fileGroup_S.start();
        // }

        // GROUPBY
        // for (int i = 0; i < runs; i++) {

        // Function: GroupBy
        long time_start = System.currentTimeMillis(); // Start Crono
        JavaPairRDD<String, Iterable<Cells>> groups = rddRev
                .groupBy(new FunctionGroupByRev());
        JavaPairRDD<String, Integer> counts = groups
                .mapToPair(new FunctionMapRevGroupBy());

        // List<Tuple2<String, Integer>> results = counts.collect();
        long results = counts.count();

        // System.out.println("\r\n Resultados GroupBy " + results + "\r\n");

        // for (Tuple2 <String,Integer> tuple:results){
        // System.out.println("\r\n Contributor "
        // +tuple._1()+" numero de articulos a su nombre " +tuple._2()+ "\r\n");
        // }

        long time_end = System.currentTimeMillis(); // End crono
        double tT = (time_end - time_start) / 1000d;
        // data[i] = tT;
        // }

        System.out.println("Count GroupBy: " + results);

        File FileTimes_G = new File(pathG);
        FileWriter TextOutTime_G = new FileWriter(FileTimes_G, true);
        TextOutTime_G.write("RESPONSE TIME GROUPBY: " + bisecFactor + ": " + tT
                + "\n");
        TextOutTime_G.close();
        // Calculate max
        // Bench benchMaxG = new Bench();
        // String.format("%.2f", benchMaxG.max(data, runs));

        // Calculate min
        // Bench benchMinG = new Bench();
        // String.format("%.2f", benchMinG.min(data, runs));

        // Calculate avg
        // Bench benchAvgG = new Bench();
        // String.format("%.2f", benchAvgG.media(data, runs));

        // stop files Master for Group

        // fileGroup_M.stop();
        // stop files Slaves for Group
        // for (int i = 0; i < slaves.size(); i++) {
        // FileGroup fileGroup_S = new FileGroup(slaves.get(i), pathFileG);
        // fileGroup_S.stop();
        // }

    }
}
