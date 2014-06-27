package com.stratio.deep.test;

import java.io.*;

/**
 * Created by ParadigmaTecnologico on 22/05/2014.
 */
public class Bench {

    public static void main(String[] args) throws IOException {


        File f;
        f = new File("C:\\Users\\ParadigmaTecnologico\\prueba.txt");

        //Escritura

        try{
            FileWriter w = new FileWriter(f);
            BufferedWriter bw = new BufferedWriter(w);
            PrintWriter wr = new PrintWriter(bw);
            wr.write("Esta es una linea de codigo");//escribimos en el archivo
            wr.append(" - y aqui continua............."); //concatenamos en el archivo sin borrar lo existente
            //ahora cerramos los flujos de canales de datos, al cerrarlos el archivo quedará guardado con información escrita
            //de no hacerlo no se escribirá nada en el archivo
            wr.close();
            bw.close();
        }catch(IOException e){};

        /*
        try {

            //Creamos un Nuevo objeto FileWriter dandole
            //como parámetros la ruta y nombre del fichero
            FileWriter fichero = new FileWriter("C:\\Users\\ParadigmaTecnologico\\prueba.txt");

            //Insertamos el texto creado y si trabajamos
            //en Windows terminaremos cada línea con "\r\n"
            fichero.write("texto prueba" + "\r\n");

            //cerramos el fichero
            fichero.close();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        */
    }
}




