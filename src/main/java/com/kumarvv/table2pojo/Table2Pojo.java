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
package com.kumarvv.table2pojo;

import com.kumarvv.table2pojo.core.PojoWriter;
import com.kumarvv.table2pojo.core.TableReader;
import com.kumarvv.table2pojo.model.UserPrefs;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.IntStream;

public class Table2Pojo {

    /**
     * construct and initialize que
     */
    public Table2Pojo() {
    }

    /**
     * start process
     * @param args
     */
    public static void main(String[] args) {
        new Table2Pojo().process(args);
    }

    /**
     * process
     * @param args
     */
    private void process(String[] args) {
        UserPrefs prefs = buildOptions(args);
        if (prefs == null) {
            return;
        }

        if (!validate(prefs)) {
            return;
        }

        long millis = System.currentTimeMillis();

        info("connecting to database...");
        try (Connection conn = connect()) {
            millis = System.currentTimeMillis();
            info("processing tables...");
            start(prefs, conn);
        } catch (Exception e) {
            error(e.getMessage());
        } finally {
            long elapsed = System.currentTimeMillis() -millis;
            info("ALL DONE! (elapsed: " + elapsed + "ms)");
        }
    }

    /**
     * build commandline options
     * @param args
     * @return
     */
    private UserPrefs buildOptions(String[] args) {
        try {
            Options options = new Options();
            options.addOption("a", "all", false, "generate POJOs for all the tables in database");
            options.addOption("t", "tables", true, "list of database tables delimited by ; (semicolon). overrides `a` option");
            options.addOption("p", "pkg", true, "(optional) java package name of the POJOs. If not specified, default/blank package will be used");
            options.addOption("d", "dir", true, "(optional) target directory where POJOs (.Java files) are generated. If not specified, current directory will be used");
            options.addOption("r", "threads", true, "(optional) number of concurrent threads, default 5");
            options.addOption("h", "help", false, "print help");

            CommandLineParser parser = new DefaultParser();
            CommandLine line = parser.parse(options, args);

            if (line.hasOption("h")) {
                HelpFormatter help = new HelpFormatter();
                help.printHelp("java -jar table2pojo-all.jar <options>", options);
                return null;
            }

            UserPrefs prefs = new UserPrefs();
            if (line.hasOption("a")) {
                prefs.setAllTables(true);
                info("tables=all");
            } else if (line.hasOption("t")) {
                prefs.setAllTables(false);
                prefs.setTables(line.getOptionValues("t"));
                info("tables=" + Arrays.toString(prefs.getTables()));
            }

            if (line.hasOption("p")) {
                prefs.setPkg(line.getOptionValue("p"));
                info("package=" + prefs.getPkg());
            }

            if (line.hasOption("d")) {
                prefs.setDir(line.getOptionValue("d"));
                info("directory=" + prefs.getDir());
            }

            if (line.hasOption("r")) {
                prefs.setNumThreads(Integer.valueOf(line.getOptionValue("r")));
            }
            info("numThreads=" + prefs.getNumThreads());

            System.out.println("--------------------------------------------");
            return prefs;

        } catch (Exception e) {
            error(e.getMessage());
            return null;
        }
    }

    /**
     * validate user preferences
     * @param prefs
     * @return
     */
    private boolean validate(final UserPrefs prefs) {
        if (prefs == null) {
            error("invalid user preferences");
            return false;
        }

        if (!prefs.isAllTables() && ArrayUtils.isEmpty(prefs.getTables())) {
            error("choose \"all\" or \"tables\" option with list of tables");
            return false;
        }

        return true;
    }

    /**
     * connect
     * @return
     * @throws Exception
     */
    private Connection connect() throws Exception {
        Properties props = new Properties();
        props.load(new FileInputStream(getDbProperties()));

        String driver = props.getProperty("driver");
        String url = props.getProperty("url");
        String username = props.getProperty("username");
        String password = props.getProperty("password");

        Class.forName(driver);

        return DriverManager.getConnection(url, username, password);
    }

    /**
     * check if valid file
     * @return
     */
    protected String getDbProperties() {
        String path = "db.properties";

        File f = Paths.get(path).toFile();
        if (!f.exists()) {
            error("Invalid file path: " + f.getAbsolutePath());
            return null;
        }

        return f.getAbsolutePath();
    }

    /**
     * start process
     * @param prefs
     * @param conn
     */
    protected void start(final UserPrefs prefs, final Connection conn) {
        if (prefs == null || conn == null) {
            return;
        }

        final BlockingQueue<String> queue = new LinkedBlockingDeque<>();

        TableReader reader = new TableReader(prefs, conn, queue);
        reader.setName("reader");
        reader.start();

        final List<PojoWriter> writers = new ArrayList<>();
        IntStream.range(0, prefs.getNumThreads()).forEach(i -> {
            writers.add(new PojoWriter(prefs, conn, queue, i));
        });

        writers.forEach(Thread::start);

        try {
            reader.join();
        } catch (InterruptedException ie) {}

        writers.forEach(w -> {
            try {
                w.join();
            } catch (InterruptedException ie) {
            }
        });
    }

    /**
     * error print
     * @param msg
     */
    private void error(String msg) {
        System.out.println("ERROR: " + msg);
    }

    /**
     * info print
     * @param msg
     */
    private void info(String msg) {
        System.out.println("INFO: " + msg);
    }
}
