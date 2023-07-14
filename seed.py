from neo4j import GraphDatabase

# credentials
uri="bolt://localhost:7687"
user="neo4j"
password="password"

# connection to neo4j
conn = GraphDatabase.driver(uri, auth=(user, password))

# session
session = conn.session()    

# insert data from books.csv
def seedBooks():
    with open("books.csv", "r") as f:
        for line in f.readlines()[1:]:
            book_id, title, author, publication_date = line.split(",")
            session.run(
                "CREATE (:Book {book_id: $book_id, title: $title, author: $author, publication_date: $publication_date})",
                {
                    "book_id": int(book_id),
                    "title": title,
                    "author": author,
                    "publication_date": publication_date
                }
            )
    return

# insert data from borrowers.csv
def seedBorrowers():
    with open("borrowers.csv", "r") as f:
        for line in f.readlines()[1:]:
            borrower_id, name, email = line.split(",")
            session.run(
                "CREATE (:Borrower {borrower_id: $borrower_id, name: $name, email: $email})",
                {
                    "borrower_id": int(borrower_id),
                    "name": name,
                    "email": email,
                }
            )
    return

# insert data from borrowing_history.csv
def seedBorrowingHistory():
    with open("borrowing_history.csv", "r") as f:
        for line in f.readlines()[1:]:
            borrowing_id, book_id, borrower_id, borrow_date, return_date = line.split(",")
            session.run(
                """
                MATCH (book:Book {book_id: $book_id})
                MATCH (borrower:Borrower {borrower_id: $borrower_id})
                CREATE (borrower)-[:BORROWED {borrowing_id: $borrowing_id, borrow_date: $borrow_date, return_date: $return_date}]->(book)
                """,
                {
                    "borrowing_id": int(borrowing_id),
                    "book_id": int(book_id),
                    "borrower_id": int(borrower_id),
                    "borrow_date": borrow_date,
                    "return_date": return_date
                }
            )
    return

seedBooks()
seedBorrowers()
seedBorrowingHistory()

# close connection
session.close()