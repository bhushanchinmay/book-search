package com.h2;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URI;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <ul>
 * <li><strong>Batch Processing:</strong> Instead of inserting records one by
 * one (which requires a network round-trip for every row),
 * we group them into batches (e.g., 1000 at a time). This reduces network
 * overhead by ~1000x.</li>
 * <li><strong>Transaction Management (ACID):</strong> We purposefully disable
 * auto-commit (`conn.setAutoCommit(false)`).
 * We only `commit()` when a batch is successfully processed. If an error
 * occurs, we `rollback()`, ensuring our database
 * is never left in a half-broken state.</li>
 * <li><strong>Structured Logging (SLF4J):</strong> We avoid
 * `System.out.println`. Loggers allow us to control output verbosity
 * (INFO vs DEBUG vs ERROR) and format logs with timestamps automatically, which
 * is critical for debugging in production.</li>
 * <li><strong>Configuration Object:</strong> We don't hardcode credentials
 * inline. We wrap them in a `Config` record
 * initialized from Environment Variables. This adheres to the "Twelve-Factor
 * App" methodology for cloud-native apps.</li>
 * </ul>
 */
public class DBImporter {

    private static final Logger logger = LoggerFactory.getLogger(DBImporter.class);

    // BATCH_SIZE: Balancing memory usage vs performance.
    // Too small = too many network trips. Too large = OutOfMemoryError or
    // transaction timeouts.
    private static final int BATCH_SIZE = 1000;

    /**
     * Infrastructure Configuration Pattern.
     * <p>
     * <strong>Why?</strong>
     * Separating configuration from logic makes the code testable (we can pass mock
     * configs)
     * and secure (we don't commit secrets to git).
     */
    private record Config(String dbUrl, String dbUser, String dbPassword, String csvUrl) {
        static Config fromEnv() {
            // SECURITY BEST PRACTICE:
            // Never hardcode defaults for secrets (like passwords) in source code.
            // If the environment variable is missing, we must fail fast to alert the
            // operator.

            String dbName = getEnvOrThrow("POSTGRES_DB");
            String dbUser = getEnvOrThrow("POSTGRES_USER");
            String dbPass = getEnvOrThrow("POSTGRES_PASSWORD");

            return new Config(
                    "jdbc:postgresql://localhost:5432/" + dbName,
                    dbUser,
                    dbPass,
                    "https://gist.github.com/hhimanshu/d55d17b51e0a46a37b739d0f3d3e3c74/raw/5b9027cf7b1641546c1948caffeaa44129b7db63/books.csv");
        }

        private static String getEnvOrThrow(String key) {
            String val = System.getenv(key);
            if (val == null || val.isBlank()) {
                throw new IllegalStateException("Missing required environment variable: " + key);
            }
            return val;
        }
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        logger.info("Starting Data Import Process...");

        Config config = Config.fromEnv();

        try {
            // Step 1: Fetch and Parse CSV
            // We separate "fetching" from "persisting" to adhere to Single Responsibility
            // Principle (SRP).
            logger.info("Downloading and Parsing CSV from: {}", config.csvUrl);
            List<String[]> records = fetchAndParseCsv(config.csvUrl);
            logger.info("Successfully parsed {} records from CSV.", records.size());

            if (records.isEmpty()) {
                logger.warn("No records found in CSV. Aborting import.");
                return;
            }

            // Step 2: Persist to Database
            // Try-with-resources: Ensures the Connection is *always* closed, even if the
            // app crashes.
            // This prevents "Connection Leaks" which can freeze a production database.
            logger.info("Connecting to database: {}", config.dbUrl);
            try (Connection conn = DriverManager.getConnection(config.dbUrl, config.dbUser, config.dbPassword)) {
                persistData(conn, records);
            }

            long duration = System.currentTimeMillis() - startTime;
            logger.info("Data Import Completed Successfully in {} ms.", duration);

        } catch (Exception e) {
            // We catch generic Exception at the top level to ensure we log a FATAL error
            // before the process dies.
            logger.error("Critical Failure during Data Import", e);
            System.exit(1);
        }
    }

    private static List<String[]> fetchAndParseCsv(String csvUrl) throws IOException, CsvValidationException {
        URI uri = URI.create(csvUrl);
        HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
        conn.setConnectTimeout(5000); // Fail fast: Don't hang forever if the network is down.
        conn.setReadTimeout(10000);

        // Handling Redirects (HTTP 301/302) manually if needed guarantees robustness.
        if (conn.getResponseCode() >= 300 && conn.getResponseCode() < 400) {
            String location = conn.getHeaderField("Location");
            if (location != null) {
                logger.debug("Redirecting to: {}", location);
                return fetchAndParseCsv(location); // Recursion
            }
        }

        try (InputStream inputStream = conn.getInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {

            CSVParser parser = new CSVParserBuilder().withSeparator(',').build();
            // We use OpenCSV instead of manual string splitting because CSVs are complex
            // (e.g., quoted values like "Book Title, The" containing commas).
            try (CSVReader csvReader = new CSVReaderBuilder(reader).withCSVParser(parser).build()) {
                List<String[]> records = new ArrayList<>();
                String[] line;
                while ((line = csvReader.readNext()) != null) {
                    records.add(line);
                }
                return records;
            }
        }
    }

    private static void persistData(Connection conn, List<String[]> records) throws SQLException {
        // DISABLING AUTO-COMMIT is the first step of a Transaction.
        // It tells the DB: "Don't save anything until I explicitly say commit()".
        conn.setAutoCommit(false);

        // SQL Injection Protection: ALWAYS use PreparedStatement.
        // Never concatenate strings like "INSERT ... VALUES ('" + val + "')".
        // PreparedStatement treats inputs strictly as data, not executable code.
        final String INSERT_AUTHOR = "INSERT INTO authors (name) VALUES (?) ON CONFLICT (name) DO NOTHING";
        final String SELECT_AUTHOR = "SELECT author_id FROM authors WHERE name = ?";

        // We use ON CONFLICT DO NOTHING (Idempotency).
        // This allows us to run the import script 100 times without duplicating data or
        // crashing.
        final String INSERT_BOOK_CLEAN = "INSERT INTO books (book_id, title, rating, description, language, isbn, book_format, edition, pages, publisher, publish_date, first_publish_date, liked_percent, price) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (book_id) DO NOTHING";
        final String INSERT_BOOK_AUTHOR = "INSERT INTO books_authors (book_id, author_id) VALUES (?, ?) ON CONFLICT DO NOTHING";

        try (PreparedStatement authorStmt = conn.prepareStatement(INSERT_AUTHOR);
                PreparedStatement selectAuthorStmt = conn.prepareStatement(SELECT_AUTHOR);
                PreparedStatement bookStmt = conn.prepareStatement(INSERT_BOOK_CLEAN);
                PreparedStatement bookAuthorStmt = conn.prepareStatement(INSERT_BOOK_AUTHOR)) {

            String[] header = records.get(0);
            Map<String, Integer> map = buildHeaderMap(header);

            // SubList view is efficient; it doesn't copy the underlying array.
            List<String[]> dataRows = records.subList(1, records.size());

            int count = 0;

            logger.info("Phase 1: Syncing Authors...");
            // We sync authors first so we have the Foreign Keys (author_id) ready for the
            // join table.
            Map<String, Integer> authorCache = syncAuthors(conn, dataRows, map, authorStmt, selectAuthorStmt);

            logger.info("Phase 2: Syncing Books and Relationships...");
            for (String[] record : dataRows) {
                String bookIdStr = record[map.get("bookId")];
                Long bookId = parseLongSafe(bookIdStr); // Parse to Long

                String authorName = record[map.get("author")];
                Integer authorId = authorCache.get(authorName);

                if (authorId == null) {
                    logger.warn("Skipping record with missing author ID for: {}", authorName);
                    continue;
                }

                // --- BATCHING ---
                // Instead of executing the query immediately, we add it to a local buffer.

                // Book Data
                bookStmt.setLong(1, bookId);
                bookStmt.setString(2, record[map.get("title")]);
                bookStmt.setBigDecimal(3, parseBigDecimalSafe(record[map.get("rating")]));
                bookStmt.setString(4, record[map.get("description")]);
                bookStmt.setString(5, record[map.get("language")]);
                bookStmt.setString(6, record[map.get("isbn")]);
                bookStmt.setString(7, record[map.get("bookFormat")]);
                bookStmt.setString(8, record[map.get("edition")]);
                bookStmt.setInt(9, parseIntSafe(record[map.get("pages")]));
                bookStmt.setString(10, record[map.get("publisher")]);
                bookStmt.setDate(11, parseDateSafe(record[map.get("publishDate")]));
                bookStmt.setDate(12, parseDateSafe(record[map.get("firstPublishDate")]));

                bookStmt.setBigDecimal(13, parseBigDecimalSafe(record[map.get("likedPercent")]));
                bookStmt.setBigDecimal(14, parseBigDecimalSafe(record[map.get("price")]));
                bookStmt.addBatch(); // Add to buffer

                // Relationship Data
                bookAuthorStmt.setLong(1, bookId);
                bookAuthorStmt.setInt(2, authorId);
                bookAuthorStmt.addBatch(); // Add to buffer

                // Flush the buffer to the DB every BATCH_SIZE records.
                if (++count % BATCH_SIZE == 0) {
                    bookStmt.executeBatch(); // Send all books in one network packet
                    bookAuthorStmt.executeBatch(); // Send all relationships in one packet
                    conn.commit(); // Save progress. If crash happens later, these 1000 are safe.
                    logger.info("Processed {} records...", count);
                }
            }

            // Flush any remaining records (e.g. if total is 2005, flush the last 5).
            bookStmt.executeBatch();
            bookAuthorStmt.executeBatch();
            conn.commit();
            logger.info("Final Commit. Total records processed: {}", count);

        } catch (SQLException e) {
            // ATOMICITY: Or "All or Nothing".
            // If any error happens in the middle of a batch, we undo (rollback) everything
            // since the last commit.
            // This prevents "partial data" corruption.
            conn.rollback();
            logger.error("Transaction Rolled Back due to error", e);
            throw e;
        } finally {
            // Always return connection to "default" state before closing/returning to pool.
            conn.setAutoCommit(true);
        }
    }

    private static Map<String, Integer> syncAuthors(Connection conn, List<String[]> rows, Map<String, Integer> map,
            PreparedStatement insertStmt, PreparedStatement selectStmt) throws SQLException {
        // Caching: We keep authors in memory to avoid querying the DB 20,000 times for
        // the same 50 authors.
        Map<String, Integer> cache = new HashMap<>();

        for (String[] row : rows) {
            String name = row[map.get("author")];
            if (name == null || name.isBlank())
                continue;

            if (!cache.containsKey(name)) {
                // Check DB
                selectStmt.setString(1, name);
                try (ResultSet rs = selectStmt.executeQuery()) {
                    if (rs.next()) {
                        cache.put(name, rs.getInt(1));
                    } else {
                        // Insert new
                        insertStmt.setString(1, name);
                        insertStmt.executeUpdate();
                        // Retrieve the auto-generated ID
                        selectStmt.setString(1, name);
                        try (ResultSet rs2 = selectStmt.executeQuery()) {
                            if (rs2.next())
                                cache.put(name, rs2.getInt(1));
                        }
                    }
                }
            }
        }
        return cache;
    }

    private static Map<String, Integer> buildHeaderMap(String[] header) {
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < header.length; i++) {
            map.put(header[i], i);
        }
        return map;
    }

    // --- Safe Parsing Utilities ---
    // Why? Data is dirty. "N/A", "", or "null" strings will crash
    // Double.parseDouble().
    // We wrap parsing in try-catch to default to safety (0.0 or null).

    private static BigDecimal parseBigDecimalSafe(String val) {
        if (val == null || val.isBlank()) {
            return BigDecimal.ZERO;
        }
        try {
            return new BigDecimal(val);
        } catch (NumberFormatException e) {
            return BigDecimal.ZERO;
        }
    }

    private static Long parseLongSafe(String val) {
        if (val == null || val.isBlank()) {
            return 0L;
        }
        try {
            return Long.parseLong(val);
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private static int parseIntSafe(String val) {
        if (val == null || val.isBlank())
            return 0;
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static Date parseDateSafe(String val) {
        if (val == null || val.isBlank())
            return null;
        try {
            return Date.valueOf(val);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}