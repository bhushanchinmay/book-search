-- We are using postgresql 17
-- CSV Header is below
-- bookId,title,author,rating,description,language,isbn,bookFormat,edition,pages,publisher,publishDate,firstPublishDate,likedPercent,price

-- Create the books table

CREATE TABLE books (
    book_id VARCHAR(255) PRIMARY KEY,
    title VARCHAR(255),
    rating VARCHAR(255),
    description TEXT,
    language VARCHAR(255),
    isbn VARCHAR(255),
    book_format VARCHAR(255),
    edition VARCHAR(255),
    pages VARCHAR(255),
    publisher VARCHAR(255),
    publish_date VARCHAR(255),
    first_publish_date VARCHAR(255),
    liked_percent VARCHAR(255),
    price VARCHAR(255),
    search_vector tsvector
);

CREATE INDEX books_search_idx ON books USING GIN (search_vector);

CREATE FUNCTION books_search_vector_update() RETURNS trigger AS $$
BEGIN
  NEW.search_vector :=
    setweight(to_tsvector('english', coalesce(NEW.title, '')), 'A') ||
    setweight(to_tsvector('english', coalesce(NEW.description, '')), 'B') ||
    setweight(to_tsvector('english', coalesce(NEW.isbn, '')), 'C');
  RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER books_search_vector_update BEFORE INSERT OR UPDATE
ON books FOR EACH ROW EXECUTE PROCEDURE books_search_vector_update();

-- Create the authors table

CREATE TABLE authors (
    author_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE
);

-- Create the books_authors table

CREATE TABLE books_authors (
    book_id VARCHAR(255) NOT NULL,
    author_id INTEGER NOT NULL,
    PRIMARY KEY (book_id, author_id),
    FOREIGN KEY (book_id) REFERENCES books(book_id) ON DELETE CASCADE,
    FOREIGN KEY (author_id) REFERENCES authors(author_id) ON DELETE CASCADE
);
