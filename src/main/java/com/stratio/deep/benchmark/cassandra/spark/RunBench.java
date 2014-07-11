package com.stratio.deep.benchmark.cassandra.spark;

import com.stratio.deep.benchmark.cassandra.spark.filter.FunctionFilterPageCount;
import com.stratio.deep.benchmark.cassandra.spark.groupby.FunctionGroupByRev;
import com.stratio.deep.benchmark.cassandra.spark.groupby.FunctionMapRevGroupBy;
import com.stratio.deep.benchmark.cassandra.spark.join.FunctionMapPageJoin;
import com.stratio.deep.benchmark.cassandra.spark.join.FunctionMapRevJoin;
import com.stratio.deep.config.DeepJobConfigFactory;
import com.stratio.deep.config.ICassandraDeepJobConfig;
import com.stratio.deep.context.DeepSparkContext;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.rdd.CassandraJavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.hyperic.sigar.SigarException;

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
        final String pathTime = args[1];
        final String pathFileF= args[2];
        final String pathFileG= args[3];
        final String pathFileJ= args[4];
        final String keyspace = args[5];
        final String table1 = args[6];
        final String table2 = args[7];
        final List<String> slaves = Arrays.asList(args).subList(8, args.length);
        List<FileFilter> filterList= new ArrayList<>();

        double time_start, time_end, tT;
        String max, min, avg;

        // context properties
        String cluster = "spark://"+args[0]+":7077";
        String jobName = "stratioDeepExample";
        String deepPath = "/home/su/spark-deep-distribution-0.3.3";
        String pathF = args[1]+"/filterTime.txt";
        String pathG = args[1]+"/groupTime.txt";
        String pathJ = args[1]+"/joinTime.txt";
        int cassandraPort = 9160;
        //String keyspaceName = "minipedia";
        //String tableNameRevision = "revision";
        //String tableNamePage = "pagecounts";

        String pathJar = new File(RunBench.class.getProtectionDomain()
                .getCodeSource().getLocation().getPath()).getAbsolutePath();


        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster(cluster);
        sparkConf.setSparkHome(deepPath);
        sparkConf.setAppName(jobName);
        sparkConf.setJars(new String[]{pathJar});

        SparkContext sc = new SparkContext(sparkConf);

        // Creating Context
        DeepSparkContext deepContext = new DeepSparkContext(sc);

        // Configuration and initialization for Revision
        ICassandraDeepJobConfig<Cells> configRev = DeepJobConfigFactory
                .create().host(CASSANDRAHOST).rpcPort(cassandraPort)
                .keyspace(args[5]).table(args[6]).initialize();

        // Creating the RDD for Revision
        CassandraJavaRDD<Cells> rddRev = deepContext
                .cassandraJavaRDD(configRev);

        // Configuration and initialization for PageCounts
        ICassandraDeepJobConfig<Cells> configPage = DeepJobConfigFactory
                .create().host(args[0]).rpcPort(cassandraPort)
                .keyspace(args[5]).table(args[7]).initialize();

        // Creating the RDD for PageCounts
        CassandraJavaRDD<Cells> rddPage = deepContext
                .cassandraJavaRDD(configPage);

        // initialization files Master for Filter
        FileFilter fileFilter_M = new FileFilter(args[0],args[2]);
        fileFilter_M.start();
        // initialization files Slaves for Filter
        for (int i=5;  i ==  args.length; i++) {
            FileFilter fileFilter_S = new FileFilter(slaves.get(i),args[2]);
            fileFilter_S.start();
        }

        // FILTER
//        for (int i = 0; i < runs; i++) {
            time_start = System.currentTimeMillis(); // Start Crono

            // Function Filter: FunctionFilterPageCount -> count pages in a
            // limit times
            JavaRDD<Cells> filtrado = rddPage.filter(new FunctionFilterPageCount());

            System.out.println("\r\n Resultados Filter " + filtrado.count()+ "\r\n");

            time_end = System.currentTimeMillis(); // End crono
            tT = (time_end - time_start) / 1000;
//            data[i] = tT;
//        }

        File FileTimes_F = new File(pathF);
        FileWriter TextOutTime_F = new FileWriter(FileTimes_F, true);
        TextOutTime_F.write ("RESPONSE TIME FILTER: "+ tT + " ");
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
        fileFilter_M.stop();
        // stop files Slaves for Filter
        for (int i=8;  i ==  args.length; i++) {
            FileFilter fileFilter_S = new FileFilter(slaves.get(i),args[2]);
            fileFilter_S.stop();
        }

        // initialization files Master for Group
        FileGroup fileGroup_M = new FileGroup(args[0],args[3]);
        fileGroup_M.start();
        // initialization files Slaves for Group
        for (int i=8;  i ==  args.length; i++) {
            FileGroup fileGroup_S = new FileGroup(slaves.get(i),args[3]);
            fileGroup_S.start();
        }

        // GROUPBY
//        for (int i = 0; i < runs; i++) {
            time_start = System.currentTimeMillis(); // Start Crono

            // Function: GroupBy

            JavaPairRDD<String, Iterable<Cells>> groups = rddRev.groupBy(new FunctionGroupByRev());
            JavaPairRDD<String, Integer> counts = groups.mapToPair(new FunctionMapRevGroupBy());

//            List<Tuple2<String, Integer>> results = counts.collect();
            long results = counts.count();

//            System.out.println("\r\n Resultados GroupBy " + results + "\r\n");
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
        TextOutTime_G.write ("RESPONSE TIME GROUPBY: "+ tT + " ");
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
        fileGroup_M.stop();
        // stop files Slaves for Group
        for (int i=8;  i ==  args.length; i++) {
            FileGroup fileGroup_S = new FileGroup(slaves.get(i),args[3]);
            fileGroup_S.stop();
        }

        // initialization files Master for Join
        FileJoin fileJoin_M = new FileJoin(args[0],args[4]);
        fileJoin_M.start();
        // initialization files Slaves for Join
        for (int i=8;  i ==  args.length; i++) {
            FileJoin fileJoin_S = new FileJoin(slaves.get(i),args[4]);
            fileJoin_S.start();
        }

        // JOIN
//        for (int i = 0; i < runs; i++) {
            time_start = System.currentTimeMillis(); // Start Crono

            // Function Join
            // RDD Revision
            JavaPairRDD<String, String> pairsRDDRev = rddRev.mapToPair(new FunctionMapRevJoin());
//            System.out.println("\r\n Resultados Map Tabla Revision "+ pairsRDDRev.collect() + "\r\n");

            // RDD PageCounts
            JavaPairRDD<String, Integer> pairsRDDPage = rddPage.mapToPair(new FunctionMapPageJoin());
//            System.out.println("\r\n Resultados Map Tabla PageCounts "+ pairsRDDPage.collect() + "\r\n");

            // Join
            long join = pairsRDDRev.join(pairsRDDPage).count();
//          List<Tuple2<String, Tuple2<String, Integer>>> join = pairsRDDRev.join(pairsRDDPage).collect();
//            System.out.println("\r\n Resultados Join por campo titulo " + join+ "\r\n");

            time_end = System.currentTimeMillis(); // End crono
            tT = (time_end - time_start) / 1000;
//            data[i] = tT;
//        }

        File FileTimes_J = new File(pathJ);
        FileWriter TextOutTime_J = new FileWriter(FileTimes_J, true);
        TextOutTime_J.write ("RESPONSE TIME JOIN: "+ tT+ " ");
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
        for (int i=8;  i ==  args.length; i++) {
            FileJoin fileJoin_S = new FileJoin(slaves.get(i),args[4]);
            fileJoin_S.stop();
        }

        deepContext.stop();

        System.exit(0);
    }
}
