package com.h2.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.h2.entity.Book;
import com.h2.repository.BookRepository;

@Service
public class BookService {
    public static final int DEFAULT_LIMIT = 20;
    public static final int MAX_LIMIT = 100;

    @Autowired
    private BookRepository bookRepository;

    public List<Book> searchBooks(String searchTerm) {
        return searchBooks(searchTerm, DEFAULT_LIMIT);
    }

    public List<Book> searchBooks(String searchTerm, int limit) {
        if (searchTerm == null || searchTerm.isBlank()) {
            throw new IllegalArgumentException("Search term cannot be null or blank");
        }
        if (limit < 1 || limit > MAX_LIMIT) {
            throw new IllegalArgumentException("Limit must be between 1 and " + MAX_LIMIT);
        }
        return bookRepository.searchBooks(searchTerm.trim(), limit);
    }
}
