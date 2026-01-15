package com.h2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;

public class DBImporter {

    private static final String CSV_URL = "https://gist.github.com/hhimanshu/d55d17b51e0a46a37b739d0f3d3e3c74/raw/5b9027cf7b1641546c1948caffeaa44129b7db63/books.csv";
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/"
            + System.getenv().getOrDefault("POSTGRES_DB", "library");
    private static final String DB_USER = System.getenv().getOrDefault("POSTGRES_USER", "admin");
    private static final String DB_PASSWORD = System.getenv().getOrDefault("POSTGRES_PASSWORD", "admin123");

    public static void main(String[] args) {
        try {
            System.out.println("Starting data ingestion...");

            // Step 1: Download CSV Data
            System.out.println("Downloading CSV data...");
            InputStream csvStream = downloadCSV(CSV_URL);

            // Step 2: Parse CSV Data
            System.out.println("Parsing CSV data...");
            List<String[]> records = parseCSV(csvStream);
            System.out.println("Parsed " + records.size() + " records.");

            // Step 3: Insert Data into Database
            System.out.println("Inserting data into database...");
            insertData(records);

            System.out.println("Data ingestion completed successfully.");

        } catch (Exception e) {
            System.out.println("An error occurred during data ingestion: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Download the CSV file from the URL
    private static InputStream downloadCSV(String urlStr) throws IOException {
        URI uri = URI.create(urlStr);
        URL url = uri.toURL();
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        return conn.getInputStream();
    }

    // Preprocess and parse the CSV file using OpenCSV
    private static List<String[]> parseCSV(InputStream csvStream) throws IOException, CsvValidationException {
        List<String[]> records = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(csvStream));

        CSVParser parser = new CSVParserBuilder()
                .withSeparator(',')
                .build();

        CSVReader csvReader = new CSVReaderBuilder(reader)
                .withCSVParser(parser)
                .build(); // Don't skip the header

        String[] nextLine;
        while ((nextLine = csvReader.readNext()) != null) {
            records.add(nextLine);
        }

        csvReader.close();
        return records;
    }

    // Insert data into the database, including authors and book_authors tables
    private static void insertData(List<String[]> records) throws SQLException {
        Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);

        String insertAuthorSQL = "INSERT INTO authors (name) VALUES (?) ON CONFLICT (name) DO NOTHING RETURNING author_id";
        String selectAuthorSQL = "SELECT author_id FROM authors WHERE name = ?";
        String insertBookSQL = "INSERT INTO books (book_id, title, rating, description, language, isbn, book_format, edition, pages, publisher, publish_date, first_publish_date, liked_percent, price) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String insertBookAuthorSQL = "INSERT INTO books_authors (book_id, author_id) VALUES (?, ?)";

        conn.setAutoCommit(false);

        try (PreparedStatement authorStmt = conn.prepareStatement(insertAuthorSQL);
                PreparedStatement selectAuthorStmt = conn.prepareStatement(selectAuthorSQL);
                PreparedStatement bookStmt = conn.prepareStatement(insertBookSQL);
                PreparedStatement bookAuthorStmt = conn.prepareStatement(insertBookAuthorSQL)) {

            String[] header = records.get(0);
            Map<String, Integer> headerMap = new HashMap<>();
            for (int i = 0; i < header.length; i++) {
                headerMap.put(header[i], i);
            }

            for (int i = 1; i < records.size(); i++) {
                String[] record = records.get(i);

                String bookId = record[headerMap.get("bookId")];
                String title = record[headerMap.get("title")];
                String authorName = record[headerMap.get("author")];
                double rating = Double.parseDouble(record[headerMap.get("rating")]);
                String description = record[headerMap.get("description")];
                String language = record[headerMap.get("language")];
                String isbn = record[headerMap.get("isbn")];
                String bookFormat = record[headerMap.get("bookFormat")];
                String edition = record[headerMap.get("edition")];
                int pages = Integer.parseInt(record[headerMap.get("pages")]);
                String publisher = record[headerMap.get("publisher")];
                String publishDateStr = record[headerMap.get("publishDate")];
                String firstPublishDateStr = record[headerMap.get("firstPublishDate")];
                double likedPercent = Double.parseDouble(record[headerMap.get("likedPercent")]);
                double price = Double.parseDouble(record[headerMap.get("price")]);

                // Insert into authors
                int authorId;
                authorStmt.setString(1, authorName);
                ResultSet authorRS = authorStmt.executeQuery();
                if (authorRS.next()) {
                    authorId = authorRS.getInt("author_id");
                } else {
                    // Author already exists, retrieve ID
                    selectAuthorStmt.setString(1, authorName);
                    ResultSet selectAuthorRS = selectAuthorStmt.executeQuery();
                    if (selectAuthorRS.next()) {
                        authorId = selectAuthorRS.getInt("author_id");
                    } else {
                        throw new SQLException("Failed to retrieve author_id for " + authorName);
                    }
                    selectAuthorRS.close();
                }
                authorRS.close();

                // Insert into books
                // Insert only if not exists (rudimentary check or duplicate error handling
                // needed, but CSV implies unique bookId)
                bookStmt.setString(1, bookId);
                bookStmt.setString(2, title);
                bookStmt.setDouble(3, rating);
                bookStmt.setString(4, description);
                bookStmt.setString(5, language);
                bookStmt.setString(6, isbn);
                bookStmt.setString(7, bookFormat);
                bookStmt.setString(8, edition);
                bookStmt.setInt(9, pages);
                bookStmt.setString(10, publisher);

                // Handle possible null dates
                Date publishDate = null;
                Date firstPublishDate = null;
                try {
                    publishDate = Date.valueOf(publishDateStr);
                } catch (Exception e) {
                    // Date parsing failed, set to null
                }
                try {
                    firstPublishDate = Date.valueOf(firstPublishDateStr);
                } catch (Exception e) {
                    // Date parsing failed, set to null
                }
                bookStmt.setDate(11, publishDate);
                bookStmt.setDate(12, firstPublishDate);

                bookStmt.setDouble(13, likedPercent);
                bookStmt.setDouble(14, price);
                bookStmt.executeUpdate();
                // No need to get generated keys as we inserted the ID explicitly

                // Insert into book_authors
                bookAuthorStmt.setString(1, bookId);
                bookAuthorStmt.setInt(2, authorId);
                bookAuthorStmt.executeUpdate();
            }
            conn.commit();
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(true);
            conn.close();
        }
    }
}