package com.stratio.deep;

import com.stratio.deep.config.DeepJobConfigFactory;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.context.DeepSparkContext;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.rdd.CassandraJavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.hyperic.sigar.SigarException;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;

/**
 * Created by ParadigmaTecnologico on 22/05/2014.
 */

public class RunBench {


    public static void main(String[] args) throws IOException, SigarException {


            int runs=2; //EXECUTIONS NUMBER
            double time_start, time_end, tT;
            double data[] = new double[runs];
            String max,min,avg;


            // context properties
            String cluster = "spark://172.19.0.207:7077";
            //String cluster = "http://172.19.0.207:8080/";
            String jobName = "stratioDeepExample";
            String deepPath = "/home/su/spark-deep-distribution-0.2.5";
            String jar = "/home/su/bench/target/test-proyect-1.0-SNAPSHOT.jar";
            String cassandraHost = "172.19.0.207";
            int cassandraPort = 9160;
            //int cassandraPort = 9042;
            String keyspaceName = "minipedia";
            String tableNameRevision = "revision";
            String tableNamePage = "pagecounts";

        //Creating Context
        DeepSparkContext deepContext = new DeepSparkContext(cluster, jobName, deepPath, jar);

        // Configuration and initialization for Revision
       IDeepJobConfig<Cells> configRev = DeepJobConfigFactory.create()
                .host(cassandraHost).rpcPort(cassandraPort)
                .keyspace(keyspaceName).table(tableNameRevision)
                .initialize();

        // Creating the RDD for Revision
        CassandraJavaRDD<Cells> rddRev = deepContext.cassandraJavaRDD(configRev);

        // Configuration and initialization for PageCounts
        IDeepJobConfig<Cells> configPage = DeepJobConfigFactory.create()
                .host(cassandraHost).rpcPort(cassandraPort)
                .keyspace(keyspaceName).table(tableNamePage)
                .initialize();

        // Creating the RDD for PageCounts
        CassandraJavaRDD<Cells> rddPage = deepContext.cassandraJavaRDD(configPage);

        //Starting Benchmark
        FileFilter fileFilterF = new FileFilter();
        fileFilterF.start();

            //FILTER
            for (int i=0; i<runs; i++){
                time_start =  System.currentTimeMillis(); // Start Crono

                //Function Filter: FunctionFilterPageCount -> count pages in a limit times
                JavaRDD<Cells> filtrado = rddPage.filter(new FunctionFilterPageCount());

                System.out.println("\r\n Resultados Filter "+ filtrado.count()+ "\r\n");

                time_end = System.currentTimeMillis(); //End crono
                tT=(time_end - time_start)/1000;
                data[i]= tT;
                }

                System.out.println("\r\n Filter Times \r\n");
                //Calculate max
                Bench benchMaxF = new Bench();
                String.format("%.2f", benchMaxF.max(data, runs));

                //Calculate min
                Bench benchMinF = new Bench();
                String.format("%.2f", benchMinF.min(data, runs));

                //Calculate avg
                Bench benchAvgF= new Bench();
                String.format("%.2f",benchAvgF.media(data,runs));

        fileFilterF.stop();
        FileGroup hiloG = new FileGroup();
        hiloG.start();
            //GROUPBY
            for (int i=0; i<runs; i++){
                time_start =  System.currentTimeMillis(); // Start Crono

                //Function: GroupBy

                JavaPairRDD<String, List<Cells>> groups = rddRev.groupBy(new FunctionGroupByRev());
                JavaPairRDD<String,Integer> counts = groups.map(new FunctionMapRevGroupBy());

                List<Tuple2<String, Integer>> results = counts.collect();

                System.out.println("\r\n Resultados GroupBy "+ results+ "\r\n");
                /*
                for (Tuple2 <String,Integer> tuple:results){
                    System.out.println("\r\n Contributor "+tuple._1()+" numero de articulos a su nombre " +tuple._2()+  "\r\n");
                }
                */

                time_end = System.currentTimeMillis(); //End crono
                tT=(time_end - time_start)/1000;
                data[i]= tT;
            }

            System.out.println("\r\n GroupBy Times \r\n");
            //Calculate max
            Bench benchMaxG = new Bench();
            String.format("%.2f", benchMaxG.max(data, runs));

            //Calculate min
            Bench benchMinG = new Bench();
            String.format("%.2f", benchMinG.min(data, runs));

            //Calculate avg
            Bench benchAvgG= new Bench();
            String.format("%.2f",benchAvgG.media(data,runs));

        hiloG.stop();
        FileJoin hiloJ = new FileJoin();
        hiloJ.start();
        //JOIN
        for (int i=0; i<runs; i++){
            time_start =  System.currentTimeMillis(); // Start Crono

            //Function Join
            //RDD Revision
            JavaPairRDD<String, String> pairsRDDRev = rddRev.map(new FunctionMapRevJoin());
            System.out.println("\r\n Resultados Map Tabla Revision "+ pairsRDDRev.collect() +"\r\n");

            //RDD PageCounts
            JavaPairRDD<String, Integer> pairsRDDPage = rddPage.map(new FunctionMapPageJoin());
            System.out.println("\r\n Resultados Map Tabla PageCounts "+ pairsRDDPage.collect() +"\r\n");

            //Join
            List<Tuple2<String, Tuple2<String, Integer>>> join = pairsRDDRev.join(pairsRDDPage).collect();
            System.out.println("\r\n Resultados Join por campo titulo "+ join+ "\r\n");

            time_end = System.currentTimeMillis(); //End crono
            tT=(time_end - time_start)/1000;
            data[i]= tT;
        }

            System.out.println("\r\n Join Times \r\n");
            //Calculate max
            Bench benchMaxJ = new Bench();
            String.format("%.2f", benchMaxJ.max(data, runs));

            //Calculate min
            Bench benchMinJ = new Bench();
            String.format("%.2f", benchMinJ.min(data, runs));

            //Calculate avg
            Bench benchAvgJ= new Bench();
            String.format("%.2f",benchAvgJ.media(data,runs));

        hiloJ.stop();
/*
        try {

             //****METRICAS

            System.out.println ("Metricas del Main ");
            ReturnValue check_disk_all  = jnrpeClient.sendCommand("check_disk_all"); //"check_diks_all" --> operaciones del agent.ini (/home/su/agentServer-0.1.0-alpha/conf)
            Metric metric = MetricParser.generateResult(check_disk_all.getMessage(), check_disk_all.getStatus().getSeverity());
            List<MetricValue> list = metric.getMetricValues();//Sacar la operacion que interese

            for (MetricValue matriz:list){
            System.out.println(matriz);
             }

        } catch (Exception e) {
        }
*/

        deepContext.stop();
/**************************************************************************
        //calcular maximo
        Bench benchMax = new Bench();
        String.format("%.2f", benchMax.max(data, runs));


        //calcular min
        Bench benchMin = new Bench();
        String.format("%.2f", benchMin.min(data, runs));


        //calcular media
        Bench benchAvg= new Bench();
        String.format("%.2f",benchAvg.media(data,runs));
***************************************************************************/
/*
//Informacion del SO

        InfoSO infoso = new InfoSO();
        infoso.imprimirInfo();
        infoso.imprimirUptime();

//Informacion CPU
        InfoCpu infocpu = new InfoCpu();
        infocpu.imprimirInfoCPU();

//Informacion de memoria

        InfoMem infomem = new InfoMem();
        infomem.imprimirInfo();
*/
       /*
        try {
                FileWriter fichero = new FileWriter("C:\\Users\\ParadigmaTecnologico\\prueba.txt");

                fichero.write("|*******************************************************************************************************|" + "\r\n");
                fichero.write("|==========================================|    unit    |      min      |      max      |      avg      |" + "\r\n");
                fichero.write("|                                                                                                       |" + "\r\n");
                fichero.write("| nombredelafuncionmaslargadelahistoria    |    ");
                fichero.write("seg     |       ");
                fichero.write(max + "       |       ");
                fichero.write(min + "       |       ");
                fichero.write(avg + "       |       ");

                fichero.close();

            } catch (Exception ex) {
                ex.printStackTrace();
            }
        */
        System.exit (0);
    }
}
