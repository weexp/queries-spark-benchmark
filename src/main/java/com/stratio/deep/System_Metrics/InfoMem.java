package com.stratio.deep.System_Metrics;

/**
 * Created by ParadigmaTecnologico on 26/05/2014.
 */
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.Swap;
import org.hyperic.sigar.SigarException;

public class InfoMem {
    private Sigar sigar = new Sigar();
    public void imprimirInfo() throws SigarException {
        System.out.println ("***Memory information***");
        Mem memoria = sigar.getMem();
        Swap intercambio = sigar.getSwap();
        System.out.println("Cantidad de memoria RAM: "+ (memoria.getRam()) + " MB");
        System.out.println("Total: "+enBytes(memoria.getTotal()) + " Bytes");
        System.out.println("Usada: "+enBytes(memoria.getUsed()) + " Bytes");
        System.out.println("Disponible: "+enBytes(memoria.getFree()) + " Bytes");
        System.out.println("Memoria SWAP total: "+enBytes(intercambio.getTotal())+ " Bytes");
        System.out.println("Memoria SWAP usada: "+enBytes(intercambio.getUsed())+ " Bytes");
        System.out.println("Memoria SWAP libre: "+enBytes(intercambio.getFree())+ " Bytes");
        System.out.println();
    }
    private Long enBytes(long valor) {
        return new Long(valor / 1024);
    }
}
