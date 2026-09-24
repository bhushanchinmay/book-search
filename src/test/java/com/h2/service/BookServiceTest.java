package com.h2.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import com.h2.TestcontainersConfiguration;
import com.h2.entity.Book;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
public class BookServiceTest {

    @Autowired
    private BookService bookService;

    @Test
    void testSearchBooksWhenTermIsEmpty() {
        String searchTerm = "";
        assertThrows(IllegalArgumentException.class, () -> bookService.searchBooks(searchTerm));
    }

    @Test
    void testSearchBooksWhenTermIsNull() {
        String searchTerm = null;
        assertThrows(IllegalArgumentException.class, () -> bookService.searchBooks(searchTerm));
    }

    @Test
    void testSearchBooksWhenTermIsBlank() {
        String searchTerm = "   ";
        assertThrows(IllegalArgumentException.class, () -> bookService.searchBooks(searchTerm));
    }

    @Test
    void testSearchBooksWhenLimitIsOutOfRange() {
        assertThrows(IllegalArgumentException.class, () -> bookService.searchBooks("algorithms", 0));
        assertThrows(IllegalArgumentException.class,
                () -> bookService.searchBooks("algorithms", BookService.MAX_LIMIT + 1));
    }

    @Test
    void testSearchBooksWhenTermIsValid() {
        String searchTerm = "algorithms";
        List<Book> books = bookService.searchBooks(searchTerm);
        assertTrue(books.size() > 0);
    }

    @Test
    void testSearchBooksWithLimit() {
        List<Book> books = bookService.searchBooks("algorithms", 1);
        assertEquals(1, books.size());
    }
}
