package com.h2.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.h2.entity.Book;

public interface BookRepository extends JpaRepository<Book, Long> {

    // websearch_to_tsquery accepts free-form user input ("data structures", "\"exact phrase\"", "-excluded")
    // without raising syntax errors. The 'english' config matches the one used by the search_vector trigger.
    @Query(value = """
            SELECT * FROM books
            WHERE search_vector @@ websearch_to_tsquery('english', :searchTerm)
            ORDER BY ts_rank(search_vector, websearch_to_tsquery('english', :searchTerm)) DESC, book_id
            LIMIT :limit
            """, nativeQuery = true)
    List<Book> searchBooks(@Param("searchTerm") String searchTerm, @Param("limit") int limit);
}
