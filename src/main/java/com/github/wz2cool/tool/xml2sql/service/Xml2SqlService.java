package com.github.wz2cool.tool.xml2sql.service;

import com.github.wz2cool.tool.xml2sql.model.DatabaseInfo;
import com.github.wz2cool.tool.xml2sql.model.DbTable;
import com.github.wz2cool.tool.xml2sql.model.SqlTemplate;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

public class Xml2SqlService {
    private Xml2SqlService() {
    }

    public static DbTable generateDbTableByXml(String xmlFilepath, int searchRowCount)
            throws IOException, XMLStreamException {
        DbTable result = new DbTable();
        List<String> columnNames = result.getColumns();
        int rowFlag = 0;

        XMLInputFactory inputFactory = XMLInputFactory.newInstance();
        try (InputStream in = new FileInputStream(xmlFilepath)) {
            XMLStreamReader streamReader = inputFactory.createXMLStreamReader(in);
            streamReader.nextTag();
            result.setTableName(streamReader.getLocalName());
            try {
                while (streamReader.hasNext()) {
                    if (streamReader.isStartElement()
                            && StringUtils.equalsIgnoreCase("row", streamReader.getLocalName())) {
                        for (int attInd = 0; attInd < streamReader.getAttributeCount(); attInd++) {
                            String name = streamReader.getAttributeLocalName(attInd);

                            if (!columnNames.contains(name)) {
                                columnNames.add(name);
                            }
                        }
                    }

                    if (searchRowCount < rowFlag) {
                        break;
                    }
                    streamReader.next();
                    rowFlag++;
                }
            } finally {
                streamReader.close();
            }
        }
        return result;
    }

    private volatile static boolean isFinished = false;
    private static Object locker = new Object();

    public static void xmlToDb(String xmlFilepath, DatabaseInfo databaseInfo)
            throws IOException, XMLStreamException, SQLException, ClassNotFoundException {

        List<String[]> queues = new ArrayList<>();
        /*  ExecutorService exService = Executors.newSingleThreadExecutor();*/

        DbTable dbTable = generateDbTableByXml(xmlFilepath, 10000);
        String tableName = dbTable.getTableName();
        List<String> columnNames = dbTable.getColumns();
        String[] columnPlaceholders = columnNames.stream().map(x -> String.format("`%s`", x)).toArray(String[]::new);
        String columnPlaceholder = String.join(",", columnPlaceholders);
        String[] valuePlaceholders = columnNames.stream().map(x -> "?").toArray(String[]::new);
        String valuePlaceholder = String.join(",", valuePlaceholders);
        String insertSql = String.format("INSERT INTO `%s`(%s) VALUES (%s)", tableName, columnPlaceholder, valuePlaceholder);
/*
        exService.execute(new Runnable() {
            @Override
            public void run() {
                while (!isFinished || !queues.isEmpty()) {
                    try {
                        int size = queues.size();
                        if (size % 1000 == 0 || isFinished) {
                            System.out.println("insert to db");
                            List<String[]> entities = new ArrayList<>();
                            if (queues.drainTo(entities) > 0) {
                                insertToDb(databaseInfo, insertSql, entities);
                            }
                        }
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
        });*/


        XMLInputFactory inputFactory = XMLInputFactory.newInstance();
        try (InputStream in = new FileInputStream(xmlFilepath)) {
            XMLStreamReader streamReader = inputFactory.createXMLStreamReader(in);
            try {
                while (streamReader.hasNext()) {
                    if (streamReader.isStartElement()
                            && StringUtils.equalsIgnoreCase("row", streamReader.getLocalName())) {
                        Map<String, String> rowMap = new HashMap<>();
                        for (int attInd = 0; attInd < streamReader.getAttributeCount(); attInd++) {
                            String name = streamReader.getAttributeLocalName(attInd);
                            String value = streamReader.getAttributeValue(attInd);
                            rowMap.put(name, value);
                        }
                        String[] params = new String[columnNames.size()];
                        for (int i = 0; i < columnNames.size(); i++) {
                            String columnName = columnNames.get(i);
                            String value = rowMap.getOrDefault(columnName, null);
                            params[i] = value;
                        }

                        queues.add(params);
                        if (queues.size() % 1000 == 0) {
                            System.out.println("begin insert");
                            insertToDb(databaseInfo, insertSql, queues);
                            queues.clear();
                        }
                    }
                    streamReader.next();
                }

            } finally {
                streamReader.close();
            }
        }

        insertToDb(databaseInfo, insertSql, queues);
        queues.clear();

    }

    private static long total = 0;

    private static synchronized void insertToDb(
            final DatabaseInfo databaseInfo, final String insertSql, final List<String[]> entities)
            throws SQLException, ClassNotFoundException {
        Class.forName("com.mysql.cj.jdbc.Driver");
        try (Connection conn = DriverManager.getConnection(
                databaseInfo.getDatabaseUrl(), databaseInfo.getUsername(), databaseInfo.getPassword())) {
            conn.setAutoCommit(false);
            try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                for (String[] entity : entities) {
                    for (int i = 0; i < entity.length; i++) {
                        String value = entity[i];
                        pstmt.setString(i + 1, value);
                    }
                    pstmt.addBatch();
                }
                int[] effectRows = pstmt.executeBatch();
                conn.commit();
                total = total + effectRows.length;
                System.out.println("success insert:" + total);
            }
        }

    }

   /* public static void xmlToSql(String xmlFilepath, String sqlFilepath) throws IOException, XMLStreamException {
        DbTable dbTable = generateDbTableByXml(xmlFilepath, 10000);
        String tableName = dbTable.getTableName();
        List<String> columnNames = dbTable.getColumns();
        String[] columnPlaceholders = columnNames.stream().map(x -> String.format("`%s`", x)).toArray(String[]::new);
        String columnPlaceholder = String.join(",", columnPlaceholders);
        StringBuilder stringBuilder = new StringBuilder();
        File sqlFile = new File(sqlFilepath);

        // save inert
        String insertExpression = String.format("INSERT INTO `%s`(%s) VALUES", tableName, columnPlaceholder);
        FileUtils.writeStringToFile(sqlFile, insertExpression, "utf8", true);
        int rowFlag = 0;
        XMLInputFactory inputFactory = XMLInputFactory.newInstance();
        try (InputStream in = new FileInputStream(xmlFilepath)) {
            XMLStreamReader streamReader = inputFactory.createXMLStreamReader(in);
            try {
                while (streamReader.hasNext()) {
                    if (streamReader.isStartElement()
                            && StringUtils.equalsIgnoreCase("row", streamReader.getLocalName())) {
                        Map<String, String> rowMap = new HashMap<>();
                        for (int attInd = 0; attInd < streamReader.getAttributeCount(); attInd++) {
                            String name = streamReader.getAttributeLocalName(attInd);
                            String value = streamReader.getAttributeValue(attInd);
                            rowMap.put(name, value);
                        }
                        stringBuilder.append("(");
                        for (int i = 0; i < columnNames.size(); i++) {
                            String columnName = columnNames.get(i);
                            String value = rowMap.getOrDefault(columnName, null);
                            String useValue = value == null
                                    ? null : String.format("\"%s\"", StringEscapeUtils.escapeJava(value));
                            if (i == columnNames.size() - 1) {
                                stringBuilder.append(useValue);
                            } else {
                                stringBuilder.append(useValue).append(",");
                            }
                        }
                        stringBuilder.append("),");
                        rowFlag++;
                        if (rowFlag % 100000 == 0) {
                            System.out.println(rowFlag);
                            FileUtils.writeStringToFile(sqlFile, stringBuilder.toString(), "utf8", true);
                            stringBuilder.setLength(0);
                        }
                    }
                    streamReader.next();
                }
                FileUtils.writeStringToFile(sqlFile, StringUtils.stripEnd(stringBuilder.toString(), ","), "utf8", true);
            } finally {
                streamReader.close();
            }
        }
    }*/
}

