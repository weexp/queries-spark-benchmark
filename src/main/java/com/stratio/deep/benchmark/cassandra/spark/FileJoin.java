package com.stratio.deep.benchmark.cassandra.spark;


import com.stratio.deep.Metrics.MetricValue;

import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

/**
 * Created by ParadigmaTecnologico on 26/05/2014.
 */
public class FileJoin extends Thread{

    private String ip, path;
    public FileJoin (String ip, String path){
        this.ip = ip;
        this.path = path;
    }

    public void run() {


        //JNRPEClient jnrpeClient = new JNRPEClient("172.19.0.207", 5666, false);

        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss:SSS");
        //String path = "/home/su/bench_copy/bench/Logs/Join/";
        //int dia = Integer.parseInt(Integer.toString(calendar.get(Calendar.DATE)));
        //int mes = Integer.parseInt(Integer.toString(calendar.get(Calendar.MONTH)));
        //int annio = Integer.parseInt(Integer.toString(calendar.get(Calendar.YEAR)));
        int t = 1;
        File FileCpu = new File(path +"cpu.txt");
        File FileDisk = new File(path +"disk.txt");
        File FileMemFree = new File(path +"mem_free.txt");
        File FileMemSwap = new File(path +"mem_swap.txt");

        Bench bench = new Bench (ip);

        while (t == 1) {

            try {
                /****Bench Methods****/
                List<MetricValue> disk = bench.disk();
                List<MetricValue> cpu = bench.cpu();
                List<MetricValue> mem_free = bench.mem_free();
                List<MetricValue> mem_swap = bench.mem_swap();
                /****Bench Methods****/

             /*METRICAS*/
                /*
                ReturnValue check_disk_all = jnrpeClient.sendCommand("check_disk_all"); //"check_diks_all" --> operaciones del agent.ini (/home/su/agentServer-0.1.0-alpha/conf)
                Metric metric = MetricParser.generateResult(check_disk_all.getMessage(), check_disk_all.getStatus().getSeverity());
                List<MetricValue> list = metric.getMetricValues();//Sacar la operacion que interese
                */
                /**********/
                //Thread.sleep(2000);
                /*
                for (MetricValue matriz : list) {
                    System.out.println(matriz);
                }
                */

                try {
                    FileWriter TextOutCpu = new FileWriter(FileCpu, true);
                    FileWriter TextOutDisk = new FileWriter(FileDisk, true);
                    FileWriter TextOutMemFree = new FileWriter(FileMemFree, true);
                    FileWriter TextOutMemSwap = new FileWriter(FileMemSwap, true);


                    TextOutDisk.write (ip+ " "+sdf.format(Calendar.getInstance().getTime()));
                    TextOutDisk.write(disk +"\r\n");
                    TextOutCpu.write (ip+ " "+sdf.format(Calendar.getInstance().getTime()));
                    TextOutCpu.write(cpu +"\r\n");
                    TextOutMemFree.write (ip+ " "+sdf.format(Calendar.getInstance().getTime()));
                    TextOutMemFree.write(mem_free +"\r\n");
                    TextOutMemSwap.write (ip+ " "+sdf.format(Calendar.getInstance().getTime()));
                    TextOutMemSwap.write(mem_swap +"\r\n");

                    TextOutCpu.close();
                    TextOutDisk.close();
                    TextOutMemFree.close();
                    TextOutMemSwap.close();


                } catch (Exception e){}

            } catch (Exception e) {}
        }





    }
}

