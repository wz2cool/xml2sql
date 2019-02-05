package com.github.wz2cool.tool.xml2sql.service;

import com.github.wz2cool.tool.xml2sql.model.DbTable;
import org.apache.commons.lang3.StringUtils;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;

public class Xml2SqlService {
    private Xml2SqlService() {
    }

    public static DbTable generateDbTableByXml(String xmlFilepath, int searchRowCount) throws Exception {
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
}

