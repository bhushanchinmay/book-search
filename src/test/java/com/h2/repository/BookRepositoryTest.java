package com.h2.repository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.context.annotation.Import;

import com.h2.TestcontainersConfiguration;
import com.h2.entity.Book;

@Import(TestcontainersConfiguration.class)
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
public class BookRepositoryTest {

    @Autowired
    private BookRepository bookRepository;

    @Autowired
    private TestEntityManager testEntityManager;

    // Rows are inserted inside the test transaction, which @DataJpaTest rolls back afterwards.
    // Negative IDs cannot clash with imported books.
    private void insertBook(long bookId, String title, String description) {
        testEntityManager.getEntityManager()
                .createNativeQuery("INSERT INTO books (book_id, title, description) VALUES (?, ?, ?)")
                .setParameter(1, bookId)
                .setParameter(2, title)
                .setParameter(3, description)
                .executeUpdate();
    }

    private List<Long> ids(List<Book> books) {
        return books.stream().map(Book::getBookId).toList();
    }

    @Test
    void testSearchBooks() {
        List<Book> books = bookRepository.searchBooks("algorithms", 20);
        assertTrue(books.size() > 0);
    }

    @Test
    void testSearchBooksWithMultipleWords() {
        insertBook(-1, "Zyxquux Plimbot Handbook", null);
        insertBook(-2, "Zyxquux Only", null);

        assertEquals(List.of(-1L), ids(bookRepository.searchBooks("zyxquux plimbot", 20)));
    }

    @Test
    void testSearchBooksToleratesQuerySyntaxCharacters() {
        insertBook(-1, "Zyxquux Plimbot Handbook", null);

        assertEquals(List.of(-1L), ids(bookRepository.searchBooks("zyxquux's & | ! ( plimbot:*", 20)));
    }

    @Test
    void testSearchBooksOrdersByRank() {
        // Title matches are weighted 'A', description matches 'B', so the title match must come first
        // even though it was inserted last.
        insertBook(-1, "Unrelated Title", "A book that mentions zyxquux in passing");
        insertBook(-2, "Zyxquux", null);

        assertEquals(List.of(-2L, -1L), ids(bookRepository.searchBooks("zyxquux", 20)));
    }

    @Test
    void testSearchBooksRespectsLimit() {
        insertBook(-1, "Zyxquux One", null);
        insertBook(-2, "Zyxquux Two", null);
        insertBook(-3, "Zyxquux Three", null);

        assertEquals(2, bookRepository.searchBooks("zyxquux", 2).size());
    }
}
