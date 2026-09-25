package com.h2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class DBImporterTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = TestcontainersConfiguration.newPostgresContainer();

    private static final String[] HEADER = { "bookId", "title", "author", "rating", "description", "language",
            "isbn", "bookFormat", "edition", "pages", "publisher", "publishDate", "firstPublishDate", "likedPercent",
            "price" };

    private Connection conn;

    @BeforeEach
    void setUp() throws SQLException {
        conn = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("TRUNCATE books, authors, books_authors RESTART IDENTITY");
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        conn.close();
    }

    private static String[] row(String bookId, String author, String rating, String pages, String publishDate,
            String price) {
        return new String[] { bookId, "Title " + bookId, author, rating, "Description", "English", "978-0",
                "Paperback", "1st", pages, "Publisher", publishDate, "2000-01-01", "90", price };
    }

    private void importRows(String[]... rows) throws SQLException {
        List<String[]> records = new ArrayList<>();
        records.add(HEADER);
        records.addAll(List.of(rows));
        DBImporter.persistData(conn, records);
    }

    private long count(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    @Test
    void importsBooksWithTheirAuthors() throws SQLException {
        importRows(row("1", "Ada Lovelace", "4.5", "300", "2020-11-06", "74.67"),
                row("2", "Ada Lovelace", "3.9", "120", "1991-01-13", "9.99"));

        assertEquals(2, count("SELECT count(*) FROM books"));
        assertEquals(1, count("SELECT count(*) FROM authors"));
        assertEquals(2, count("SELECT count(*) FROM books_authors"));
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT rating, pages, publish_date, price FROM books WHERE book_id = 1")) {
            rs.next();
            assertEquals(new BigDecimal("4.50"), rs.getBigDecimal("rating"));
            assertEquals(300, rs.getInt("pages"));
            assertEquals(Date.valueOf("2020-11-06"), rs.getDate("publish_date"));
            assertEquals(new BigDecimal("74.67"), rs.getBigDecimal("price"));
        }
    }

    @Test
    void storesMissingOrInvalidValuesAsNullNotZero() throws SQLException {
        importRows(row("1", "Ada Lovelace", "", "N/A", "2021-02-30", "free"));

        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT rating, pages, publish_date, price FROM books WHERE book_id = 1")) {
            rs.next();
            assertNull(rs.getBigDecimal("rating"));
            assertNull(rs.getObject("pages"));
            assertNull(rs.getDate("publish_date"));
            assertNull(rs.getBigDecimal("price"));
        }
    }

    @Test
    void skipsRowsWithInvalidBookIdInsteadOfUsingZero() throws SQLException {
        importRows(row("abc", "Ada Lovelace", "4.5", "300", "2020-11-06", "10"),
                row("", "Ada Lovelace", "4.5", "300", "2020-11-06", "10"),
                row("7", "Ada Lovelace", "4.5", "300", "2020-11-06", "10"));

        assertEquals(1, count("SELECT count(*) FROM books"));
        assertEquals(0, count("SELECT count(*) FROM books WHERE book_id = 0"));
        assertEquals(1, count("SELECT count(*) FROM books WHERE book_id = 7"));
    }

    @Test
    void keepsBooksWithoutAnAuthor() throws SQLException {
        importRows(row("1", "", "4.5", "300", "2020-11-06", "10"),
                row("2", "Ada Lovelace", "4.5", "300", "2020-11-06", "10"));

        assertEquals(2, count("SELECT count(*) FROM books"));
        assertEquals(1, count("SELECT count(*) FROM books_authors"));
        assertEquals(0, count("SELECT count(*) FROM books_authors WHERE book_id = 1"));
    }

    @Test
    void reimportingIsIdempotent() throws SQLException {
        String[] book = row("1", "Ada Lovelace", "4.5", "300", "2020-11-06", "10");
        importRows(book);
        importRows(book);

        assertEquals(1, count("SELECT count(*) FROM books"));
        assertEquals(1, count("SELECT count(*) FROM authors"));
        assertEquals(1, count("SELECT count(*) FROM books_authors"));
    }
}
