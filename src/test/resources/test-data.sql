-- Books the tests rely on. Loaded after db/create_schema.sql by TestcontainersConfiguration.
-- Two titles match "algorithms" so tests can check that `limit` actually cuts results.

INSERT INTO books (book_id, title, rating, description, isbn, pages, publish_date, price) VALUES
    (1, 'Introduction to Algorithms', 4.35, 'A comprehensive textbook on algorithms and data structures.', '9780262033848', 1312, '2009-07-31', 89.99),
    (2, 'Algorithms to Live By', 4.14, 'The computer science of human decisions.', '9781627790369', 368, '2016-04-19', 17.00),
    (3, 'Clean Code', 4.38, 'A handbook of agile software craftsmanship.', '9780132350884', 464, '2008-08-01', 39.99);
