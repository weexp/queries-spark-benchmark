package com.stratio.deep.benchmark.cassandra.spark;

import com.stratio.deep.*;
import com.stratio.deep.config.DeepJobConfigFactory;
import com.stratio.deep.config.ICassandraDeepJobConfig;
import com.stratio.deep.context.DeepSparkContext;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.rdd.CassandraJavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.hyperic.sigar.SigarException;
import scala.Tuple2;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by ParadigmaTecnologico on 22/05/2014.
 */

public class RunBench {

    public static void main(String[] args) throws IOException, SigarException {
        final String CASSANDRAHOST = args[0];
        final List<String> slaves = Arrays.asList(args).subList(1, args.length);
        List<FileFilter> filterList= new ArrayList<>();

        double time_start, time_end, tT;
        String max, min, avg;

        // context properties
        String cluster = "spark://172.19.0.42:7077";
        String jobName = "stratioDeepExample";
        String deepPath = "/home/su/spark-deep-distribution-0.3.3";
        String jar = "/home/su/bench/target/test-proyect-1.0-SNAPSHOT.jar";
        String pathF = "/home/su/bench_copy/bench/Logs/times/filterTime.txt";
        String pathG = "/home/su/bench_copy/bench/Logs/times/groupTime.txt";
        String pathJ = "/home/su/bench_copy/bench/Logs/times/joinTime.txt";
        int cassandraPort = 9160;
        String keyspaceName = "minipedia";
        String tableNameRevision = "revision";
        String tableNamePage = "pagecounts";

        // Creating Context
        DeepSparkContext deepContext = new DeepSparkContext(cluster, jobName,
                deepPath, jar);

        // Configuration and initialization for Revision
        ICassandraDeepJobConfig<Cells> configRev = DeepJobConfigFactory
                .create().host(args[0]).rpcPort(cassandraPort)
                .keyspace(keyspaceName).table(tableNameRevision).initialize();

        // Creating the RDD for Revision
        CassandraJavaRDD<Cells> rddRev = deepContext
                .cassandraJavaRDD(configRev);

        // Configuration and initialization for PageCounts
        ICassandraDeepJobConfig<Cells> configPage = DeepJobConfigFactory
                .create().host(args[0]).rpcPort(cassandraPort)
                .keyspace(keyspaceName).table(tableNamePage).initialize();

        // Creating the RDD for PageCounts
        CassandraJavaRDD<Cells> rddPage = deepContext
                .cassandraJavaRDD(configPage);

        // initialization files Master for Filter
        FileFilter fileFilterF_M = new FileFilter(args[0]);
        fileFilterF_M.start();
        // initialization files Slaves for Filter
        for (int i=0;  i ==  args.length; i++) {
            FileFilter fileFilter_S1 = new FileFilter(slaves.get(i));
            fileFilter_S1.start();
        }

        // FILTER
//        for (int i = 0; i < runs; i++) {
            time_start = System.currentTimeMillis(); // Start Crono

            // Function Filter: FunctionFilterPageCount -> count pages in a
            // limit times
            JavaRDD<Cells> filtrado = rddPage
                    .filter(new FunctionFilterPageCount());

            System.out.println("\r\n Resultados Filter " + filtrado.count()
                    + "\r\n");

            time_end = System.currentTimeMillis(); // End crono
            tT = (time_end - time_start) / 1000;
//            data[i] = tT;
//        }

        File FileTimes_F = new File(pathF);
        FileWriter TextOutTime_F = new FileWriter(FileTimes_F, true);
        TextOutTime_F.write ("RESPONSE TIME FILTER: "+ tT);
        TextOutTime_F.close();
        // Calculate max
//        Bench benchMaxF = new Bench();
//        String.format("%.2f", benchMaxF.max(data, runs));

        // Calculate min
//        Bench benchMinF = new Bench();
//        String.format("%.2f", benchMinF.min(data, runs));

        // Calculate avg
//        Bench benchAvgF = new Bench();
//        String.format("%.2f", benchAvgF.media(data, runs));

        // stop files Master for Filter
        fileFilterF_M.stop();
        // stop files Slaves for Filter
        for (int i=0;  i ==  args.length; i++) {
            FileFilter fileFilter_S1 = new FileFilter(slaves.get(i));
            fileFilter_S1.stop();
        }

        // initialization files Master for Group
        FileGroup fileFilterG_M = new FileGroup(args[0]);
        fileFilterG_M.start();
        // initialization files Slaves for Group
        for (int i=0;  i ==  args.length; i++) {
            FileGroup fileGroup_S1 = new FileGroup(slaves.get(i));
            fileGroup_S1.start();
        }

        // GROUPBY
//        for (int i = 0; i < runs; i++) {
            time_start = System.currentTimeMillis(); // Start Crono

            // Function: GroupBy

            JavaPairRDD<String, Iterable<Cells>> groups = rddRev.groupBy(new FunctionGroupByRev());
            JavaPairRDD<String, Integer> counts = groups.mapToPair(new FunctionMapRevGroupBy());

            List<Tuple2<String, Integer>> results = counts.collect();

            System.out.println("\r\n Resultados GroupBy " + results + "\r\n");
            /*
             * for (Tuple2 <String,Integer> tuple:results){
             * System.out.println("\r\n Contributor "
             * +tuple._1()+" numero de articulos a su nombre " +tuple._2()+
             * "\r\n"); }
             */

            time_end = System.currentTimeMillis(); // End crono
            tT = (time_end - time_start) / 1000;
//            data[i] = tT;
//        }

        File FileTimes_G = new File(pathG);
        FileWriter TextOutTime_G = new FileWriter(FileTimes_G, true);
        TextOutTime_G.write ("RESPONSE TIME GROUPBY: "+ tT);
        TextOutTime_G.close();
        // Calculate max
//        Bench benchMaxG = new Bench();
//        String.format("%.2f", benchMaxG.max(data, runs));

        // Calculate min
//        Bench benchMinG = new Bench();
//        String.format("%.2f", benchMinG.min(data, runs));

        // Calculate avg
//        Bench benchAvgG = new Bench();
//        String.format("%.2f", benchAvgG.media(data, runs));

        // stop files Master for Group
        fileFilterG_M.stop();
        // stop files Slaves for Group
        for (int i=0;  i ==  args.length; i++) {
            FileGroup fileGroup_S1 = new FileGroup(slaves.get(i));
            fileGroup_S1.stop();
        }

        // initialization files Master for Join
        FileJoin fileJoin_M = new FileJoin(args[0]);
        fileJoin_M.start();
        // initialization files Slaves for Join
        for (int i=0;  i ==  args.length; i++) {
            FileJoin fileJoin_S1 = new FileJoin(slaves.get(i));
            fileJoin_S1.start();
        }

        // JOIN
//        for (int i = 0; i < runs; i++) {
            time_start = System.currentTimeMillis(); // Start Crono

            // Function Join
            // RDD Revision
            JavaPairRDD<String, String> pairsRDDRev = rddRev
                    .mapToPair(new FunctionMapRevJoin());
            System.out.println("\r\n Resultados Map Tabla Revision "
                    + pairsRDDRev.collect() + "\r\n");

            // RDD PageCounts
            JavaPairRDD<String, Integer> pairsRDDPage = rddPage
                    .mapToPair(new FunctionMapPageJoin());
            System.out.println("\r\n Resultados Map Tabla PageCounts "
                    + pairsRDDPage.collect() + "\r\n");

            // Join
            List<Tuple2<String, Tuple2<String, Integer>>> join = pairsRDDRev
                    .join(pairsRDDPage).collect();
            System.out.println("\r\n Resultados Join por campo titulo " + join
                    + "\r\n");

            time_end = System.currentTimeMillis(); // End crono
            tT = (time_end - time_start) / 1000;
//            data[i] = tT;
//        }

        File FileTimes_J = new File(pathJ);
        FileWriter TextOutTime_J = new FileWriter(FileTimes_J, true);
        TextOutTime_J.write ("RESPONSE TIME JOIN: "+ tT);
        TextOutTime_J.close();

        // Calculate max
//        Bench benchMaxJ = new Bench();
//        String.format("%.2f", benchMaxJ.max(data, runs));

        // Calculate min
//        Bench benchMinJ = new Bench();
//        String.format("%.2f", benchMinJ.min(data, runs));

        // Calculate avg
//        Bench benchAvgJ = new Bench();
//        String.format("%.2f", benchAvgJ.media(data, runs));

        // stop files Master for Join
        fileJoin_M.stop();
        // stop files Salve for Join
        for (int i=0;  i ==  args.length; i++) {
            FileJoin fileJoin_S1 = new FileJoin(slaves.get(i));
            fileJoin_S1.stop();
        }

        deepContext.stop();

        System.exit(0);
    }
}
