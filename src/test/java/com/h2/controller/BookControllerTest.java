package com.h2.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.h2.entity.Book;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class BookControllerTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void testSearchBooks() {
        String searchTerm = "algorithms";
        ResponseEntity<Book[]> response = restTemplate.getForEntity("/books/search?searchTerm={searchTerm}",
                Book[].class, searchTerm);

        Book[] books = response.getBody();
        assertNotNull(books);
        assertTrue(books.length > 0);
    }

    @Test
    void testSearchBooksWithMultipleWords() {
        // Used to fail with a 500: to_tsquery rejects plain text containing spaces.
        ResponseEntity<Book[]> response = restTemplate.getForEntity("/books/search?searchTerm={searchTerm}",
                Book[].class, "data structures");

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
    }

    @Test
    void testSearchBooksWithLimit() {
        ResponseEntity<Book[]> response = restTemplate.getForEntity(
                "/books/search?searchTerm={searchTerm}&limit=1", Book[].class, "algorithms");

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(1, response.getBody().length);
    }

    @Test
    void testSearchBooksWhenTermIsBlankReturnsBadRequest() {
        for (String searchTerm : new String[] { "", "   " }) {
            ResponseEntity<String> response = restTemplate.getForEntity("/books/search?searchTerm={searchTerm}",
                    String.class, searchTerm);

            assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
            assertFalse(response.getBody().contains("\"trace\""));
        }
    }

    @Test
    void testSearchBooksWhenLimitIsOutOfRangeReturnsBadRequest() {
        ResponseEntity<String> response = restTemplate.getForEntity(
                "/books/search?searchTerm={searchTerm}&limit=0", String.class, "algorithms");

        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
    }
}
