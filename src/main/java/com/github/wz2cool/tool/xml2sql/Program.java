package com.github.wz2cool.tool.xml2sql;

import com.github.wz2cool.tool.xml2sql.model.DatabaseInfo;
import com.github.wz2cool.tool.xml2sql.model.DbTable;
import com.github.wz2cool.tool.xml2sql.service.Xml2SqlService;

public class Program {

    private static String xmlFilePath = "E:\\Downloads\\baidu\\math.stackexchange.com\\Comments.xml";
    private static String sqlFilePath = "E:\\exportSql\\Users.sql";

    public static void main(String[] args) throws Exception {
      /*  System.out.println("hello world");
        saveToFile();
        readXml();
        System.out.println("read completed");*/

        DatabaseInfo info = new DatabaseInfo();
        info.setDatabaseUrl("jdbc:mysql://127.0.0.1:3306/math_stackexchange");
        info.setPassword("innodealing");
        info.setUsername("root");

        Xml2SqlService.xmlToDb(xmlFilePath, info);
        System.out.println("ffffffff");

    }

  /*  private static String xmlFilePath = "E:\\Downloads\\baidu\\math.stackexchange.com\\Comments.xml";
    private static ConcurrentLinkedQueue<String> insertQueue = new ConcurrentLinkedQueue<>();

    private static void readXml() throws Exception {


        File xmlFile = new File(xmlFilePath);

        XMLInputFactory inputFactory = XMLInputFactory.newInstance();
        try (InputStream in = new FileInputStream(xmlFile)) {
            XMLStreamReader streamReader = inputFactory.createXMLStreamReader(in);
            streamReader.nextTag();
            String tableName = streamReader.getLocalName();
            try {
                int rowFlag = 0;
                StringBuilder builder = new StringBuilder();
                while (streamReader.hasNext()) {
                    if (streamReader.isStartElement()
                            && StringUtils.equalsIgnoreCase("row", streamReader.getLocalName())) {
                        Map<String, String> map = new HashMap<>();
                        for (int attInd = 0; attInd < streamReader.getAttributeCount(); attInd++) {
                            String name = streamReader.getAttributeLocalName(attInd);
                            String value = streamReader.getAttributeValue(attInd);
                            String useName = String.format("`%s`", name);
                            String useValue = String.format("\"%s\"", StringEscapeUtils.escapeJava(value));
                            map.put(useName, useValue);
                        }
                        String columns = String.join(", ", map.keySet());
                        String values = String.join(", ", map.values());
                        builder.append("INSERT INTO `").append(tableName).append("`(").append(columns)
                                .append(") VALUES(").append(values).append(");").append(System.getProperty("line.separator"));
                    }

                    if (rowFlag % 10000 == 0 || !streamReader.hasNext()) {
                        System.out.println(rowFlag);
                        insertQueue.offer(builder.toString());
                        builder.setLength(0);

                     *//*   File sqlFile = new File("e:\\exportSql\\" + sqlFileName);
                        FileUtils.writeStringToFile(sqlFile, builder.toString(), "utf8", true);
                        builder.setLength(0);*//*
                    }
                    streamReader.next();
                    rowFlag++;
                }
            } finally {
                streamReader.close();
            }

        }
    }

    private static void saveToFile() {
        File xmlFile = new File(xmlFilePath);
        String sqlFileName = xmlFile.getName().replace(".xml", ".sql");

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        while (!insertQueue.isEmpty()) {
                            String value = insertQueue.poll();
                            File sqlFile = new File("e:\\exportSql\\" + sqlFileName);
                            FileUtils.writeStringToFile(sqlFile, value, "UTF-8", true);
                        }

                        Thread.sleep(500);
                    } catch (Exception e) {

                    }
                }
            }
        });
    }*/
}
