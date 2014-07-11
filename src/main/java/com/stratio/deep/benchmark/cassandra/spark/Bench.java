package com.stratio.deep.benchmark.cassandra.spark;

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


    private String ip;
    public Bench (String ip){
        this.ip = ip;
    }


    double time_start, time_end, tT, min, max, avg;


    //JNRPEClient jnrpeClient = new JNRPEClient("172.19.0.207", 5666, false);

    //MINIMUM
    public double min(double data[],int runs) {
        double minimo = data[0];

        // Nota: index start at one
        for (int j=1;j<runs; j++) {
            if(data[j]<minimo) {
                minimo = data[j];
            }
        }
        System.out.println("Memory "+minimo);

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
        System.out.println("Disk " +maximo);
        return maximo;
    }

    //AVERAGE
    public double media (double data [],int runs){
        double media=0;

       for (int j=0;j<runs;j++){
           media = (media + data[j]/runs);
       }

        System.out.println("Average " +media);
        System.out.println("**************");
        return media;
    }

    //***CHECK_DISK_ALL***/
    public List<MetricValue> disk () {

        //JNRPEClient jnrpeClient = new JNRPEClient("172.19.0.207", 5666, false);
        JNRPEClient jnrpeClient = new JNRPEClient(ip, 5666, false);

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

        //JNRPEClient jnrpeClient = new JNRPEClient("172.19.0.207", 5666, false);
        JNRPEClient jnrpeClient = new JNRPEClient(ip, 5666, false);

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


    //***CHECK_MEM_FREE***/
    public List<MetricValue> mem_free () throws JNRPEClientException {

        //JNRPEClient jnrpeClient = new JNRPEClient("172.19.0.207", 5666, false);
        JNRPEClient jnrpeClient = new JNRPEClient(ip, 5666, false);

        ReturnValue check_mem_free = jnrpeClient.sendCommand("check_mem_free");
        Metric metric = MetricParser.generateResult(check_mem_free.getMessage(), check_mem_free.getStatus().getSeverity());
        List<MetricValue> list = metric.getMetricValues();//Sacar la operacion que interese

        return list;
    }

    //***CHECK_MEM_SWAP***/
    public List<MetricValue> mem_swap () throws JNRPEClientException {

        //JNRPEClient jnrpeClient = new JNRPEClient("172.19.0.207", 5666, false);
        JNRPEClient jnrpeClient = new JNRPEClient(ip, 5666, false);

        ReturnValue check_mem_swap = jnrpeClient.sendCommand("check_mem_swap");
        Metric metric = MetricParser.generateResult(check_mem_swap.getMessage(), check_mem_swap.getStatus().getSeverity());
        List<MetricValue> list = metric.getMetricValues();//Sacar la operacion que interese

        return list;
    }

    //***CHECK_DISK_ALL***/
    public List<MetricValue> cass_cluster_name () {

        //JNRPEClient jnrpeClient = new JNRPEClient("172.19.0.207", 5666, false);
        JNRPEClient jnrpeClient = new JNRPEClient(ip, 5666, false);

        ReturnValue check_jmx_cassandra_cluster_name = null; //"check_diks_all" --> operaciones del agent.ini (/home/su/agentServer-0.1.0-alpha/conf)
        try {
            check_jmx_cassandra_cluster_name  = jnrpeClient.sendCommand("check_jmx_cassandra_cluster_name ");
        } catch (JNRPEClientException e) {
            e.printStackTrace();
        }
        Metric metric = MetricParser.generateResult(check_jmx_cassandra_cluster_name .getMessage(), check_jmx_cassandra_cluster_name .getStatus().getSeverity());
        List<MetricValue> list = metric.getMetricValues();//Sacar la operacion que interese
        return list;

    }


}

