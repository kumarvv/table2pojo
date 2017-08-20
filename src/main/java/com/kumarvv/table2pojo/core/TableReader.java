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

import com.kumarvv.table2pojo.model.UserPrefs;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.stream.IntStream;

import static com.kumarvv.table2pojo.model.UserPrefs.DONE;

public class TableReader extends Thread {

    private final UserPrefs prefs;
    private final Connection conn;
    private final BlockingQueue<String> queue;

    /**
     * requires connection and table
     * @param prefs
     * @param conn
     */
    public TableReader(final UserPrefs prefs, final Connection conn, final BlockingQueue<String> queue) {
        this.prefs = prefs;
        this.conn = conn;
        this.queue = queue;
        this.setName("reader-0");
    }

    /**
     * run the generator task
     */
    @Override
    public void run() {
        if (prefs == null || conn == null || queue == null) {
            throw new IllegalArgumentException("null values");
        }

        if (prefs.isAllTables()) {
            loadTablesDb();
        } else {
            loadTablesPrefs();
        }

        info("DONE");
    }

    /**
     * load all tables from dtabase
     */
    protected void loadTablesDb() {
        info("reading all tables from database...");
        try (ResultSet rs = conn.getMetaData().getTables(null, null, "%", new String[] {"TABLE"});){
            while (rs.next()) {
                queue.offer(rs.getString(3));
            }
        } catch (SQLException sqle) {
            error(sqle.getMessage());
        } finally {
            addDoneObjects();
        }
    }

    /**
     * load tables from user preferences
     */
    protected void loadTablesPrefs() {
        info("reading tables list from preferences...");
        try {
            Arrays.stream(prefs.getTables()).forEach(queue::offer);
        } finally {
            addDoneObjects();
        }
    }

    /**
     * add DONE objects to close the running writers
     */
    protected void addDoneObjects() {
        IntStream.range(0, prefs.getNumThreads()).forEach(i -> queue.offer(DONE));
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
