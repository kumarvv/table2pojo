/*
 * Copyright (c) 2017 Vijay Vijayaram
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software
 * and associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH
 * THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.kumarvv.table2pojo.core;

import com.kumarvv.table2pojo.model.DbColumn;
import com.kumarvv.table2pojo.model.UserPrefs;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.WordUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import static com.kumarvv.table2pojo.model.UserPrefs.DONE;

public class PojoWriter extends Thread {

    private static final String SQL_ALL = "select * from %s where 1>2";

    private static final Map<Integer, String> MAP_TYPES = new HashMap<>();
    static {
        MAP_TYPES.put(Types.CHAR, "String");
        MAP_TYPES.put(Types.VARCHAR, "String");
        MAP_TYPES.put(Types.LONGVARCHAR, "String");
        MAP_TYPES.put(Types.NUMERIC, "BigDecimal");
        MAP_TYPES.put(Types.DECIMAL, "BigDecimal");
        MAP_TYPES.put(Types.BIT, "Boolean");
        MAP_TYPES.put(Types.TINYINT, "Integer");
        MAP_TYPES.put(Types.SMALLINT, "Integer");
        MAP_TYPES.put(Types.INTEGER, "Integer");
        MAP_TYPES.put(Types.BIGINT, "Long");
        MAP_TYPES.put(Types.REAL, "Float");
        MAP_TYPES.put(Types.FLOAT, "Double");
        MAP_TYPES.put(Types.DOUBLE, "Double");
        MAP_TYPES.put(Types.BINARY, "byte[]");
        MAP_TYPES.put(Types.VARBINARY, "byte[]");
        MAP_TYPES.put(Types.LONGVARBINARY, "byte[]");
        MAP_TYPES.put(Types.DATE, "Date");
        MAP_TYPES.put(Types.TIME, "Time");
        MAP_TYPES.put(Types.TIMESTAMP, "Timestamp");
        MAP_TYPES.put(Types.CLOB, "Clob");
        MAP_TYPES.put(Types.BLOB, "Blob");
        MAP_TYPES.put(Types.ARRAY, "Array");
        MAP_TYPES.put(Types.STRUCT, "Struct");
        MAP_TYPES.put(Types.REF, "Ref");
        MAP_TYPES.put(Types.JAVA_OBJECT, "Object");
    }

    private static final Map<String, String> MAP_IMPORTS = new HashMap<>();
    static {
        MAP_IMPORTS.put("BigDecimal", "java.math.BigDecimal");
        MAP_IMPORTS.put("Date", "java.util.Date");
        MAP_IMPORTS.put("Time", "java.sql.Time");
        MAP_IMPORTS.put("Timestamp", "java.sql.Timestamp");
    }

    private static final String NEW_LINE = "\n";

    private final UserPrefs prefs;
    private final Connection conn;
    private final BlockingQueue<String> queue;

    /**
     * requires connection and table
     * @param prefs
     * @param conn
     */
    public PojoWriter(final UserPrefs prefs, final Connection conn, final BlockingQueue<String> queue, final int id) {
        this.prefs = prefs;
        this.conn = conn;
        this.queue = queue;
        this.setName("writer-" + id);
    }

    /**
     * run the generator task
     */
    @Override
    public void run() {
        if (prefs == null || conn == null || queue == null) {
            throw new IllegalArgumentException("null values");
        }

        while (true) {
            try {
                String table = queue.take();
                if (StringUtils.isBlank(table) || DONE.equalsIgnoreCase(table)) {
                    break;
                }

                processTable(conn, table);

            } catch (InterruptedException ie) {
                break;
            } catch (Exception e) {
                error(e.getMessage());
                break;
            }
        }
        info("DONE");
    }

    /**
     * process
     * @param conn
     * @param tableName
     */
    private void processTable(final Connection conn, final String tableName) {
        if (conn == null || StringUtils.isEmpty(tableName)) {
            return;
        }

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(String.format(SQL_ALL, tableName.toUpperCase()));) {

            if (rs == null) {
                throw new PojoWriterException("table not found");
            }

            final ResultSetMetaData meta = rs.getMetaData();

            final List<DbColumn> columns = new ArrayList<>();
            int count = meta.getColumnCount();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                DbColumn column = buildDbColumn(meta, i);
                if (column != null) {
                    columns.add(column);
                }
            }

            if (count == 0) {
                throw new PojoWriterException("no columns found in table");
            }

            String pojoPath = generatePojo(tableName, columns);
            info("[table=" + tableName + "] generated pojo file: " + pojoPath);

            String xmlPath = generateMyBatisXml(tableName, columns);
            info("[table=" + tableName + "] generated xml file: " + xmlPath);


        } catch (Exception e) {
            error("[table=" + tableName + "] " + e.getMessage().trim());
        }
    }

    /**
     * build column using result meta
     * @param meta
     * @param columnId
     * @return
     */
    private DbColumn buildDbColumn(ResultSetMetaData meta, int columnId) throws PojoWriterException {
        if (meta == null) {
            return null;
        }

        try {
            DbColumn column = new DbColumn();
            column.setCatelogName(meta.getCatalogName(columnId));
            column.setName(meta.getColumnName(columnId));
            column.setLabel(meta.getColumnLabel(columnId));
            column.setClassName(meta.getColumnClassName(columnId));
            column.setType(meta.getColumnType(columnId));
            column.setTableName(meta.getColumnTypeName(columnId));
            column.setDisplaySize(meta.getColumnDisplaySize(columnId));
            column.setPrecision(meta.getPrecision(columnId));
            column.setScale(meta.getScale(columnId));
            column.setSchemaName(meta.getSchemaName(columnId));
            column.setTableName(meta.getTableName(columnId));

            column.setJavaType(getJavaType(column));
            column.setJavaProperty(toCamelCase(column.getName()));

            return column;
        } catch (SQLException sqle) {
            throw new PojoWriterException(sqle.getMessage());
        }
    }

    /**
     * generate pojo
     * @param tableName
     * @param columns
     */
    private String generatePojo(final String tableName, final List<DbColumn> columns) throws PojoWriterException {
        if (tableName == null || CollectionUtils.isEmpty(columns)) {
            throw new PojoWriterException("invalid table name");
        }

        final Set<String> imports = new HashSet<>();
        final List<String> properties = new ArrayList<>();
        final List<String> methods = new ArrayList<>();

        columns.forEach(column -> {
            generatePojoColumn(column, imports, properties, methods);
        });

        StringBuilder sb = new StringBuilder();

        String pkg = prefs.getPkg();
        if (StringUtils.isBlank(pkg)) {
            pkg = "pojo";
        }
        sb.append("package ").append(pkg).append(";").append(NEW_LINE);
        sb.append(NEW_LINE);

        imports.forEach(s -> sb.append(s).append(NEW_LINE));
        sb.append(NEW_LINE);

        String pojoName = toMethodName(tableName);
        sb.append("public class ").append(pojoName);
        sb.append(" implements Serializable {").append(NEW_LINE);

        properties.forEach(s -> sb.append(s).append(NEW_LINE));
        sb.append(NEW_LINE);

        methods.forEach(s -> sb.append(s).append(NEW_LINE));

        sb.append("}");

        return writePojo(pojoName, sb.toString());
    }

    /**
     * generate pojo column details
     * @param column
     * @param imports
     * @param properties
     * @param methods
     */
    private void generatePojoColumn(final DbColumn column, final Set<String> imports, final List<String> properties, final List<String> methods) {
        if (column == null) {
            return;
        }

        imports.add("import java.io.Serializable;");
        if (MAP_IMPORTS.containsKey(column.getJavaType()) && imports != null) {
            imports.add("import " + MAP_IMPORTS.get(column.getJavaType()) + ";");
        }

        properties.add(generatePropertyLine(column));

        methods.add(generateGetterSetter(column));
    }

    /**
     * generate propertiy line
     * @param column
     * @return
     */
    protected String generatePropertyLine(final DbColumn column) {
        if (column == null) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("\tprivate ").append(column.getJavaType()).append(" ").append(toCamelCase(column.getName())).append(";");

        return sb.toString();
    }

    /**
     * generate getter/setter for column
     * @param column
     * @return
     */
    protected String generateGetterSetter(final DbColumn column) {
        if (column == null) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("\tpublic ").append(column.getJavaType()).append(" get").append(toMethodName(column.getName())).append("() {\n");
        sb.append("\t\treturn ").append(column.getJavaProperty()).append(";").append(NEW_LINE);
        sb.append("\t}\n");

        sb.append(NEW_LINE);

        sb.append("\tpublic void ").append("set").append(toMethodName(column.getName())).append("(")
                .append(column.getJavaType()).append(" ").append(column.getJavaProperty()).append(")").append(" {\n");
        sb.append("\t\tthis.").append(column.getJavaProperty()).append(" = ").append(column.getJavaProperty()).append(";").append(NEW_LINE);
        sb.append("\t}\n");

        return sb.toString();
    }

    /**
     * get java type
     * @param column
     * @return
     */
    private String getJavaType(final DbColumn column) {
        if (column == null) {
            return null;
        }

        String javaType = MAP_TYPES.get(column.getType());
        if (column.getType() == Types.NUMERIC && column.getPrecision() == 1 && column.getScale() == 0) {
            return "Boolean";
        }
        if (column.getType() == Types.NUMERIC && column.getScale() == 0) {
            return "Long";
        }

        return javaType;
    }

    /**
     * db columnName to camelCaseName
     * @param str
     * @return
     */
    private String toCamelCase(String str) {
        return WordUtils.uncapitalize(WordUtils.capitalizeFully(str, '_').replaceAll("_", ""));
    }

    /**
     * db columnName to camelCaseName
     * @param str
     * @return
     */
    private String toMethodName(String str) {
        return WordUtils.capitalizeFully(str, '_').replaceAll("_", "");
    }

    /**
     * writes pojo into directory
     * @param pojoStr
     */
    private String writePojo(String pojoName, String pojoStr) throws PojoWriterException {
        if (StringUtils.isBlank(pojoName) || StringUtils.isEmpty(pojoStr)) {
            throw new PojoWriterException("no pojo content, skipping write");
        }

        String pkg = prefs.getPkg();
        if (StringUtils.isBlank(pkg)) {
            pkg = "pojo";
        }

        String dir = prefs.getDir();
        if (StringUtils.isBlank(dir)) {
            dir = "out";
        }

        Path curr = Paths.get(".");
        Path out = Paths.get(dir, pkg.split("\\."));
        Path targetDir = curr.resolve(out).normalize();

        try {
            Files.createDirectories(targetDir);
        } catch (IOException e) {
            throw new PojoWriterException("could not create targetDir: " + targetDir.toString() + ", error: " + e.getMessage());
        }

        if (!targetDir.toFile().exists() || !targetDir.toFile().isDirectory()) {
            throw new PojoWriterException("pojo directory not exists: " + targetDir.toAbsolutePath());
        }

        Path targetFile = Paths.get(targetDir.toString(), pojoName + ".java");

        try {
            Files.write(targetFile, pojoStr.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

            return targetFile.toString();

        } catch (IOException e) {
            throw new PojoWriterException("could not write pojo file: " + e.getMessage());
        }
    }


    /**
     * generate pojo
     * @param tableName
     * @param columns
     */
    private String generateMyBatisXml(final String tableName, final List<DbColumn> columns) throws PojoWriterException {
        if (tableName == null || CollectionUtils.isEmpty(columns)) {
            throw new PojoWriterException("invalid table name");
        }

        StringBuilder sb = new StringBuilder();

        // XML header
        sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n" +
                "<!DOCTYPE mapper\n" +
                "        PUBLIC \"-//mybatis.org//DTD Mapper 3.0//EN\"\n" +
                "        \"http://mybatis.org/dtd/mybatis-3-mapper.dtd\">\n");
        sb.append(NEW_LINE);

        String pkg = prefs.getPkg();
        if (StringUtils.isBlank(pkg)) {
            pkg = "pojo";
        }
        String className = toMethodName(tableName);
        String fqdn = pkg + "." + className;

        // open Namespace
        sb.append("<mapper namespace=\"").append(className).append("\">\n");

        sb.append(generateMyBatisBaseSql(tableName, columns)).append("\n");
        sb.append(generateMyBatisList(tableName, columns, fqdn)).append("\n");
        sb.append(generateMyBatisOne(tableName, columns, fqdn)).append("\n");
        sb.append(generateMyBatisSearch(tableName, columns, fqdn)).append("\n");
        sb.append(generateMyBatisInsert(tableName, columns, fqdn)).append("\n");
        sb.append(generateMyBatisUpdate(tableName, columns, fqdn)).append("\n");
        sb.append(generateMyBatisDelete(tableName, columns, fqdn));

        // close Namespace
        sb.append("</mapper>\n");

        return writeMyBatisXml(className, sb.toString());
    }

    private String generateMyBatisBaseSql(final String tableName, final List<DbColumn> columns) throws PojoWriterException {
        if (tableName == null || CollectionUtils.isEmpty(columns)) {
            throw new PojoWriterException("invalid table name");
        }

        String tableNameU = tableName.toUpperCase();
        String tablePrefix = tableNameU.substring(0, 1);

        StringBuilder sql = new StringBuilder();

        sql.append("\t").append("<sql id=\"sql_select_all\">\n");
        sql.append("\t\tSELECT \n");

        String columnStr = columns.stream()
                .map(col -> tablePrefix + "." + col.getName())
                .collect(Collectors.joining(",\n\t\t\t", "\t\t\t", ""));
        sql.append(columnStr).append("\n");

        sql.append("\t\tFROM ").append(tableNameU).append(" ").append(tablePrefix).append("\n");
        sql.append("\t").append("</sql>\n");

        return sql.toString();
    }

    private String generateMyBatisList(final String tableName, final List<DbColumn> columns, final String fqdn) throws PojoWriterException {
        if (tableName == null || CollectionUtils.isEmpty(columns)) {
            throw new PojoWriterException("invalid table name");
        }

        StringBuilder sql = new StringBuilder();

        sql.append("\t").append("<select id=\"list\" resultType=\"").append(fqdn).append("\">\n");
        sql.append("\t\t<include refid=\"sql_select_all\"/>\n");
        sql.append("\t").append("</select>\n");

        return sql.toString();
    }

    private String generateMyBatisOne(final String tableName, final List<DbColumn> columns, final String fqdn) throws PojoWriterException {
        if (tableName == null || CollectionUtils.isEmpty(columns)) {
            throw new PojoWriterException("invalid table name");
        }

        String tableNameU = tableName.toUpperCase();
        String tablePrefix = tableNameU.substring(0, 1);

        StringBuilder sql = new StringBuilder();

        sql.append("\t").append("<select id=\"one\" parameterType=\"map\" resultType=\"").append(fqdn).append("\">\n");
        sql.append("\t\t<include refid=\"sql_select_all\"/>\n");
        sql.append("\t\t").append("WHERE ").append(tablePrefix).append(".ID = #{id}\n");
        sql.append("\t").append("</select>\n");

        return sql.toString();
    }

    private String generateMyBatisSearch(final String tableName, final List<DbColumn> columns, final String fqdn) throws PojoWriterException {
        if (tableName == null || CollectionUtils.isEmpty(columns)) {
            throw new PojoWriterException("invalid table name");
        }

        String tableNameU = tableName.toUpperCase();
        String tablePrefix = tableNameU.substring(0, 1);

        StringBuilder sql = new StringBuilder();

        sql.append("\t").append("<select id=\"search\" parameterType=\"map\" resultType=\"").append(fqdn).append("\">\n");
        sql.append("\t\t<include refid=\"sql_select_all\"/>\n");
        sql.append("\t\t").append("WHERE ").append(tablePrefix).append(".code LIKE #{value}\n");
        sql.append("\t").append("</select>\n");

        return sql.toString();
    }

    private String generateMyBatisInsert(final String tableName, final List<DbColumn> columns, final String fqdn) throws PojoWriterException {
        if (tableName == null || CollectionUtils.isEmpty(columns)) {
            throw new PojoWriterException("invalid table name");
        }

        String tableNameU = tableName.toUpperCase();
        String tablePrefix = tableNameU.substring(0, 1);
        String className = toMethodName(tableName);

        StringBuilder sql = new StringBuilder();

        sql.append("\t").append("<insert id=\"insert\" parameterType=\"").append(fqdn).append("\" useGeneratedKeys=\"true\"\n" +
                "            keyProperty=\"id\" keyColumn=\"ID\"").append(">\n");
        sql.append("\t\tINSERT INTO ").append(tableNameU).append("\n");
        sql.append("\t\t\t(\n");

        String columnsIns = columns.stream()
                .map(col -> col.getName()).collect(Collectors.joining(", "));

        sql.append("\t\t\t\t").append(columnsIns).append("\n");
        sql.append("\t\t\t)\n");
        sql.append("\t\tVALUES\n");
        sql.append("\t\t\t(\n");

        String columnsVal = columns.stream()
                .map(col -> "#{" + toCamelCase(col.getName()) + "}").collect(Collectors.joining(", "));


        sql.append("\t\t\t\t").append(columnsVal).append("\n");
        sql.append("\t\t\t)\n");
        sql.append("\t").append("</insert>\n");

        return sql.toString();
    }

    private String generateMyBatisUpdate(final String tableName, final List<DbColumn> columns, final String fqdn) throws PojoWriterException {
        if (tableName == null || CollectionUtils.isEmpty(columns)) {
            throw new PojoWriterException("invalid table name");
        }

        String tableNameU = tableName.toUpperCase();
        String className = toMethodName(tableName);

        StringBuilder sql = new StringBuilder();

        sql.append("\t").append("<update id=\"update\" parameterType=\"").append(fqdn).append("\">\n");
        sql.append("\t\tUPDATE ").append(tableNameU).append("\n");
        sql.append("\t\tSET\n");

        String columnsIns = columns.stream()
                .map(col -> col.getName() + " = #{" + toCamelCase(col.getName()) + "}")
                .collect(Collectors.joining(", \n\t\t\t"));

        sql.append("\t\t\t").append(columnsIns).append("\n");
        sql.append("\t\tWHERE ID = #{id}\n");

        sql.append("\t").append("</update>\n");

        return sql.toString();
    }

    private String generateMyBatisDelete(final String tableName, final List<DbColumn> columns, final String fqdn) throws PojoWriterException {
        if (tableName == null || CollectionUtils.isEmpty(columns)) {
            throw new PojoWriterException("invalid table name");
        }

        String tableNameU = tableName.toUpperCase();
        String tablePrefix = tableNameU.substring(0, 1);

        StringBuilder sql = new StringBuilder();

        sql.append("\t").append("<delete id=\"delete\" parameterType=\"map\">\n");
        sql.append("\t\tDELETE FROM ").append(tableNameU).append(" \n");
        sql.append("\t\t").append("WHERE ").append(tablePrefix).append(".ID = #{value}\n");
        sql.append("\t").append("</delete>\n");

        return sql.toString();
    }

    /**
     * writes pojo into directory
     * @param pojoStr
     */
    private String writeMyBatisXml(String pojoName, String pojoStr) throws PojoWriterException {
        if (StringUtils.isBlank(pojoName) || StringUtils.isEmpty(pojoStr)) {
            throw new PojoWriterException("no pojo content, skipping write");
        }

        String pkg = prefs.getPkg();
        if (StringUtils.isBlank(pkg)) {
            pkg = "pojo";
        }

        String dir = prefs.getDir();
        if (StringUtils.isBlank(dir)) {
            dir = "out";
        }

        Path curr = Paths.get(".");
        Path out = Paths.get(dir, pkg.split("\\."));
        Path targetDir = curr.resolve(out).normalize();

        try {
            Files.createDirectories(targetDir);
        } catch (IOException e) {
            throw new PojoWriterException("could not create targetDir: " + targetDir.toString() + ", error: " + e.getMessage());
        }

        if (!targetDir.toFile().exists() || !targetDir.toFile().isDirectory()) {
            throw new PojoWriterException("pojo directory not exists: " + targetDir.toAbsolutePath());
        }

        Path targetFile = Paths.get(targetDir.toString(), pojoName + ".xml");

        try {
            Files.write(targetFile, pojoStr.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

            return targetFile.toString();

        } catch (IOException e) {
            throw new PojoWriterException("could not write pojo file: " + e.getMessage());
        }
    }


    /**
     * error print
     * @param msg
     */
    private void error(String msg) {
        System.out.println("(" + getName() + ") ERROR: " + msg);
    }

    /**
     * info print
     * @param msg
     */
    private void info(String msg) {
        System.out.println("(" + getName() + ") INFO: " + msg);
    }
}
