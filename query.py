from neo4j import GraphDatabase

# credentials
uri="bolt://localhost:7687"
user="neo4j"
password="password"

# connection to neo4j
conn = GraphDatabase.driver(uri, auth=(user, password))

# session
session = conn.session()    

# query: MATCH (b:Book) RETURN b
def getAllBooks():
    result = session.run("MATCH (b:Book) RETURN b")

    # print as table with equal column width
    print("Book ID".ljust(10), "Title".ljust(50), "Author".ljust(30), "Publication Date".ljust(20))
    for record in result:
        print(str(record["b"]["book_id"]).ljust(10), record["b"]["title"].ljust(50), record["b"]["author"].ljust(30), record["b"]["publication_date"].ljust(20))

    return result

# query: MATCH (b:Book) WHERE b.publication_date STARTS WITH $year RETURN b
def getAllBooksByYear(year):
    if(year == None):
        raise ValueError("Year is required")

    result = session.run(
        """
        MATCH (b:Book)
        WHERE b.publication_date STARTS WITH $year
        RETURN b
        """,
        {
            "year": str(year)
        }
    )

    # print as table with equal column width
    print("Book ID".ljust(10), "Title".ljust(50), "Author".ljust(30), "Publication Date".ljust(20))
    for record in result:
        print(str(record["b"]["book_id"]).ljust(10), record["b"]["title"].ljust(50), record["b"]["author"].ljust(30), record["b"]["publication_date"].ljust(20))

    return result

# query: MATCH (b:Book)<-[bh:BORROWED]-(bo:Borrower) WHERE bh.borrow_date STARTS WITH $year RETURN b.title, b.author, bo.name
def getBorrowingHistoryByYear(year):
    if(year == None):
        raise ValueError("Year is required")
    
    result = session.run(
        """
        MATCH (b:Book)<-[bh:BORROWED]-(bo:Borrower)
        WHERE bh.borrow_date STARTS WITH $year
        RETURN b.title, b.author, bo.name
        """,
        {
            "year": str(year)
        }
    )

    # print as table with equal column width
    print("Title".ljust(50), "Author".ljust(30), "Borrower".ljust(20))
    for record in result:
        print(record["b.title"].ljust(50), record["b.author"].ljust(30), record["bo.name"].ljust(20))

    return result

# query: MATCH (b:Book)<-[bh:BORROWED]-(bo:Borrower) WHERE b.author = $author AND substring(bh.borrow_date, 0, 4) = $year WITH b.title AS title, b.author AS author, bh.borrow_date AS borrow_date, count(*) AS count WHERE count > (SELECT avg(borrows_per_book) FROM (MATCH (b:Book)<-[bh:BORROWED]-(bo:Borrower) WHERE substring(bh.borrow_date, 0, 4) = $year RETURN b.book_id, count(*) AS borrows_per_book GROUP BY b.book_id) AS subquery) RETURN title, author, borrow_date ORDER BY borrow_date DESC
def getBorrowingHistoryByAuthorAndBorrowYear(author, year):
    if author is None or year is None:
        raise ValueError("Author and year are required")

    # SELECT b.title, b.author, bh.borrow_date
    # FROM books b
    # JOIN borrowing_history bh ON b.book_id = bh.book_id
    # WHERE YEAR(bh.borrow_date) = 2022
    # GROUP BY b.title, b.author, bh.borrow_date
    # HAVING COUNT(*) > (
    #     SELECT AVG(borrows_per_book)
    #     FROM (
    #         SELECT COUNT(*) AS borrows_per_book
    #         FROM borrowing_history
    #         WHERE YEAR(borrow_date) = 2022
    #         GROUP BY book_id
    #     ) AS subquery
    # )
    # AND b.author = 'Rebecca Patterson'
    # ORDER BY bh.borrow_date DESC;

    # neo4j query
    result = session.run(
        """
        MATCH 
            (b:Book)<-[bh:BORROWED]-(bo:Borrower)
        WHERE
            b.author = $author 
        AND 
            substring(bh.borrow_date, 0, 4) = $year
        CALL {
            MATCH
                (b:Book)<-[bh:BORROWED]-(bo:Borrower)
            WHERE
                substring(bh.borrow_date, 0, 4) = $year
            RETURN
                count(*) AS borrows_per_book 
        }
        WITH
            b,
            bh,
            count(*) AS count,
            avg(borrows_per_book) AS avarage
        WHERE
            count > avarage
        RETURN
            b.title AS title,
            b.author AS author,
            bh.borrow_date AS borrow_date,
            count,
            avarage
        ORDER BY
            borrow_date 
        DESC
        """,
        {
            "author": author,
            "year": str(year)
        }
    )

    # print as table with equal column width
    print("Title".ljust(50), "Author".ljust(30), "Borrow Date".ljust(20), "Count".ljust(10), "Average".ljust(10))
    for record in result:
        print(record["title"].ljust(50), record["author"].ljust(30), record["borrow_date"].ljust(20), str(record["count"]).ljust(10), str(record["avarage"]).ljust(10))

    return result


# getAllBooks()
# getAllBooksByYear(2021)
# getBorrowingHistoryByYear(2023)
getBorrowingHistoryByAuthorAndBorrowYear("Kyle Rhodes", 2022)


# close connection
session.close()