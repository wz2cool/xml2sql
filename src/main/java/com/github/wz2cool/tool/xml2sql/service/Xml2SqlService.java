package com.github.wz2cool.tool.xml2sql.service;

import com.github.wz2cool.tool.xml2sql.model.DbTable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public static void xmlToSql(String xmlFilepath, String sqlFilepath) throws IOException, XMLStreamException {
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
    }
}

