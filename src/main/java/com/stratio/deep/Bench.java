package com.stratio.deep;

import com.stratio.deep.Metrics.Metric;
import com.stratio.deep.Metrics.MetricParser;
import com.stratio.deep.Metrics.MetricValue;
import it.jnrpe.ReturnValue;
import it.jnrpe.client.JNRPEClient;
import it.jnrpe.client.JNRPEClientException;

import java.util.List;

/**
 * Created by ParadigmaTecnologico on 23/05/2014.
 */
public class Bench {

    //int runs; //numero de ejecuciones
    double time_start, time_end, tT, min, max, avg;
    //long data[] = new long[10];//Array tiempos de respuesta



    //JNRPEClient jnrpeClient = new JNRPEClient("172.19.0.207", 5666, false);

    //MINIMUM
    static double min(double data[],int runs) {
        double minimo = data[0];

        // Nota: index start at one
        for (int j=1;j<runs; j++) {
            if(data[j]<minimo) {
                minimo = data[j];
            }
        }
        System.out.println("Minimo "+minimo);

        return minimo;
    }


    //MAXIMUM
    public double max (double data[], int runs) {
                double maximo = 0;

        for (int j=0; j<runs; j++) {
            if (data[j] > maximo) {
                maximo = data[j];
            }
        }
        System.out.println("**************");
        System.out.println("Maximo " +maximo);
        return maximo;
    }

    //AVERAGE
    public double media (double data [],int runs){
        double media=0;

       for (int j=0;j<runs;j++){
           media = (media + data[j]/runs);
       }

        System.out.println("Media " +media);
        System.out.println("**************");
        return media;
    }

    //***CHECK_DISK_ALL***/
    public List<MetricValue> disk () {

        //JNRPEClient jnrpeClient = new JNRPEClient(ip, port, false);
        JNRPEClient jnrpeClient = new JNRPEClient("172.19.0.207", 5666, false);

        ReturnValue check_disk_all = null; //"check_diks_all" --> operaciones del agent.ini (/home/su/agentServer-0.1.0-alpha/conf)
        try {
            check_disk_all = jnrpeClient.sendCommand("check_disk_all");
        } catch (JNRPEClientException e) {
            e.printStackTrace();
        }
        Metric metric = MetricParser.generateResult(check_disk_all.getMessage(), check_disk_all.getStatus().getSeverity());
            List<MetricValue> list = metric.getMetricValues();//Sacar la operacion que interese
        return list;

    }

    //***CHECK_CPU_LOAD***/
    public List<MetricValue> cpu (){

        JNRPEClient jnrpeClient = new JNRPEClient("172.19.0.207", 5666, false);

        ReturnValue check_cpu_load = null;
        try {
            check_cpu_load = jnrpeClient.sendCommand("check_cpu_load");
        } catch (JNRPEClientException e) {
            e.printStackTrace();
        }
        Metric metric = MetricParser.generateResult(check_cpu_load.getMessage(), check_cpu_load.getStatus().getSeverity());
        List<MetricValue> list = metric.getMetricValues();//Sacar la operacion que interese

    return list;
    }

    //***CHECK_LOAD_AVERAGE***/
    public List<MetricValue> cpu_avg (){

        JNRPEClient jnrpeClient = new JNRPEClient("172.19.0.207", 5666, false);

        ReturnValue check_load_average = null;
        try {
            check_load_average = jnrpeClient.sendCommand("check_load_average");
        } catch (JNRPEClientException e) {
            e.printStackTrace();
        }
        Metric metric = MetricParser.generateResult(check_load_average.getMessage(), check_load_average.getStatus().getSeverity());
        List<MetricValue> list = metric.getMetricValues();//Sacar la operacion que interese

    return list;
    }

    //***CHECK_MEM_FREE***/
    public List<MetricValue> mem_free () throws JNRPEClientException {

        JNRPEClient jnrpeClient = new JNRPEClient("172.19.0.207", 5666, false);

        ReturnValue check_mem_free = jnrpeClient.sendCommand("check_mem_free");
        Metric metric = MetricParser.generateResult(check_mem_free.getMessage(), check_mem_free.getStatus().getSeverity());
        List<MetricValue> list = metric.getMetricValues();//Sacar la operacion que interese

        return list;
    }

    //***CHECK_MEM_SWAP***/
    public List<MetricValue> mem_swap () throws JNRPEClientException {

        JNRPEClient jnrpeClient = new JNRPEClient("172.19.0.207", 5666, false);

        ReturnValue check_mem_swap = jnrpeClient.sendCommand("check_mem_swap");
        Metric metric = MetricParser.generateResult(check_mem_swap.getMessage(), check_mem_swap.getStatus().getSeverity());
        List<MetricValue> list = metric.getMetricValues();//Sacar la operacion que interese

        return list;
    }

    //***CHECK_DISK_STATS_ALL*/
    public List<MetricValue> disk_all () throws JNRPEClientException {

        JNRPEClient jnrpeClient = new JNRPEClient("172.19.0.207", 5666, false);

        ReturnValue check_disk_stats_all  = jnrpeClient.sendCommand("check_disk_stats_all"); //"check_diks_all" --> operaciones del agent.ini (/home/su/agentServer-0.1.0-alpha/conf)
        Metric metric = MetricParser.generateResult(check_disk_stats_all.getMessage(), check_disk_stats_all.getStatus().getSeverity());
        List<MetricValue> list = metric.getMetricValues();//Sacar la operacion que interese

        return list;
    }




    /*
    for (int i=0; i==9; i++)
        System.out.println(data[i]);

    try {
        FileWriter fichero = new FileWriter("C:\\Users\\ParadigmaTecnologico\\prueba.txt");

        fichero.write("|*******************************************************************************************************|" + "\r\n");
        fichero.write("|==========================================|    unit    |      min      |      max      |      avg      |" + "\r\n");
        fichero.write("|                                                                                                       |" + "\r\n");
        fichero.write("| "+operation+"                                         |     ");//       |               |               |               |" + "\r\n");
        fichero.write(tT + "      |       ");
        fichero.write(tT + "       |       ");
        fichero.write(tT + "       |       ");
        fichero.write(tT + "       |       ");

        fichero.close();

    } catch (Exception ex) {
        ex.printStackTrace();
    }
    */
}

