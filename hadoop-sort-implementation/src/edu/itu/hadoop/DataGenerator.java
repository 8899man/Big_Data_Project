package edu.itu.hadoop;

import java.io.*;
import java.util.*;

public class DataGenerator {

    public static final String DELIMITER = "delimiter";
    public static final String COLUMNS = "columns";
    public static final String OUT_FILE = "out";
    public static final String FILE_SIZE = "mb";

    private Map<String, String> argsMap = new HashMap<String, String>();
    private List<List<String>> columnsData;
    private String delimiter;
    private long max_file_size = 10000;
    private String out_file_name;

    private void checkArg(String name)  {
        String value = argsMap.get(name);
        if ( value == null || value.length() == 0 ) {
            throw new IllegalArgumentException("Arg " + name + " must be configured.");
        }
    }

    private void initArgs(String[] args) {
        argsMap.clear();
        for (int i = 0; i < args.length; i++) {
            if (DELIMITER.equals(args[i])) {
                argsMap.put(DELIMITER, args[++i]);
            } else if (COLUMNS.equals(args[i])) {
                argsMap.put(COLUMNS, args[++i]);
            } else if (OUT_FILE.equals(args[i])) {
                argsMap.put(OUT_FILE, args[++i]);
            } else if (FILE_SIZE.equals(args[i])) {
                argsMap.put(FILE_SIZE, args[++i]);
            }
        }
        checkArg(DELIMITER);
        checkArg(OUT_FILE);
        delimiter = argsMap.get(DELIMITER);
        out_file_name = argsMap.get(OUT_FILE);
        if (argsMap.get(FILE_SIZE) != null) {
            max_file_size = Long.parseLong(argsMap.get(FILE_SIZE)) * 1024 * 1024;
        }
    }

    private void initColumnValues() throws Exception {
        String columns = argsMap.get(COLUMNS);
        if ( columns == null ) {
            return;
        }
        String[] fileNames = columns.split(",");
        for (int i = 0; i < fileNames.length; i++) {
            loadColumn(fileNames[i]);
        }
    }

    private void loadColumn(String fileName) throws Exception {
        List<String> columnValues = new ArrayList<>();
        File file = new File(fileName);
        FileReader reader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(reader);
        String field = null;
        while ((field = bufferedReader.readLine()) != null) {
            columnValues.add(field);
        }
        bufferedReader.close();
        reader.close();
        if (columnsData == null) {
            columnsData = new ArrayList<>();
        }
        columnsData.add(columnValues);
    }

    private void generateDataToFile() throws Exception {
        FileWriter fileWriter = new FileWriter(out_file_name);
        BufferedWriter writer = new BufferedWriter(fileWriter, 1024*1024);
        long file_size = 0;
        int ind = 0;
        while ( file_size < max_file_size ) {
            String row = createRow();
            writer.append(row);
            writer.newLine();
            file_size += row.length() + 2;
            ind++;
            if (ind == 1000) {
                ind = 0;
                writer.flush();
                fileWriter.flush();
            }
        }
        writer.flush();
        fileWriter.flush();
        writer.close();
        fileWriter.close();
    }

    private void generateDataToFile2() throws Exception {
        long file_size = 0;
        int ind = 0;
        while ( file_size < max_file_size ) {
            String row = createRow();
            file_size += row.length() + 2;
            ind++;
            if (ind == 1000) {
                ind = 0;
            }
        }
    }

    private String createRandomKey() {
        StringBuilder sb = new StringBuilder(32);
        for (int i = 0; i<20; i++) {
            char c = (char) ('A' + new Random().nextInt(26));
            sb.append(c);
        }
        return sb.toString();
    }

    private String createRow() {
        StringBuilder sb = new StringBuilder(128);
        sb.append(createRandomKey());
        if ( columnsData != null ) {
            for (int i = 0; i < columnsData.size(); i++) {
                List<String> values = columnsData.get(i);
                String value = values.get(new Random().nextInt(values.size()));
                sb.append(delimiter).append(value);
            }
        }
        return sb.toString();
    }

    public static void main(String[] args) throws Exception {
        DataGenerator gen = new DataGenerator();
        gen.initArgs(args);
        gen.initColumnValues();
        long start_ms = System.currentTimeMillis();
        gen.generateDataToFile();
        long finish_ms = System.currentTimeMillis();
        System.out.println("generation time: " + (finish_ms - start_ms)/1000 + " seconds");
    }
}