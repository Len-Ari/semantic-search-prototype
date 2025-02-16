# Create Tables, Add data to tables and Query
import json
import time
import os
from pathlib import Path
from datetime import datetime

from save_load_models import load_model_sentencetransformer

model_path = './model'
model = load_model_sentencetransformer(model_path)

TABLE_EMBEDDINGS = "embeddings"
TABLE_ARTICLES = "articles"
TABLE = "pmcArticles"
NUM_DIM = 768
PRE_LIMIT = 100
LIMIT = 10
SEMANTIC_WEIGHT = 0.8
SEPERATOR = chr(11)


def create_tables(cursor):
    '''
    Execute the create Table SQL Query to create a table with predefined schema.
    This function currently deletes already existing 'articles' and 'embeddings' tabels and their content.

    Parameters:
    - cursor (pg_cursor): A cursor connected to the database to execute the sql query.
    '''
    # Drop Table if already defined
    cursor.execute(f"DROP TABLE IF EXISTS {TABLE_ARTICLES};")
    cursor.execute(f"DROP TABLE IF EXISTS {TABLE_EMBEDDINGS};")
    cursor.execute(f"DROP TABLE IF EXISTS {TABLE};")

    cursor.execute(f"DROP EXTENSION IF EXISTS vector;")
    cursor.execute(f"CREATE EXTENSION vector;")

    cursor.execute(f"DROP EXTENSION IF EXISTS pg_trgm;")
    cursor.execute(f"CREATE EXTENSION pg_trgm;")
    # Set Threshhold to speed up querying
    #cursor.execute("SET pg_trgm.similarity_threshold = 0.8;")
    '''
    cursor.execute(f"""
    CREATE TABLE {TABLE_ARTICLES} (
        pmc VARCHAR(16),
        title TEXT,
        abstract TEXT,
        journal_title VARCHAR(255),
        article_type VARCHAR(100),
        article_ids VARCHAR(50)[],
        authors VARCHAR(100)[],
        affiliations TEXT[],
        publication_dates date[]
    );
    """)
    cursor.execute(f"""
    CREATE TABLE {TABLE_EMBEDDINGS} (
        pmc VARCHAR(16),
	    type VARCHAR(8),
	    vect vector({NUM_DIM})
    );
    """)
    '''
    cursor.execute(f"""
    CREATE TABLE {TABLE} (
        pmc VARCHAR(16) PRIMARY KEY,
        title TEXT,
        abstract TEXT,
        journal_title VARCHAR,
        article_type VARCHAR,
        article_ids VARCHAR[],
        authors VARCHAR[],
        affiliations TEXT[],
        publication_dates date[],
	    ppub_date date, 
	    epub_date date, 
        title_vector vector({NUM_DIM}),
        abstract_vector vector({NUM_DIM})
);
    """)
    print("Created Tables with predefined schema.")


def createFunction(cursor):
    cursor.execute("""
    CREATE OR REPLACE FUNCTION arr_similarities(value TEXT, arr TEXT[])
    RETURNS FLOAT AS $$
    DECLARE
        max_similarity FLOAT := 0;
        total_similarity FLOAT := 0;
        current_similarity FLOAT;
    BEGIN
        FOR i IN 1..array_length(arr, 1) LOOP
            current_similarity := similarity(value, arr[i]);
            IF current_similarity > max_similarity THEN 
                max_similarity := current_similarity;
            END IF;
            total_similarity := total_similarity + current_similarity;
        END LOOP;
        RETURN max_similarity + total_similarity/array_length(arr, 1);
    END;
    $$ LANGUAGE plpgsql;
    """)
    print("Created Custom Function to use pg_trgm with arrays")



def add_to_db(cursor, dir_path):
    '''
    Recursively call add_file_to_collection for all json files in directory.
    '''
    if not os.path.exists(dir_path):
        raise Exception(f"Specified directory with path: {dir_path} does not exist!")
    if os.path.isfile(dir_path):
        if os.path.splitext(dir_path)[1] == '.jsonl':
            add_file_to_db(cursor, dir_path)
    else:
        with os.scandir(dir_path) as entries:
            for entry in entries:
                add_to_db(cursor, entry.path)


def add_file_to_db(cursor, file_path):
    '''
    Given a cursor connected to a postgres database. Add a specified (by file_path) PubMed jsonl file to it.
    (Later embedd)
    '''
    doc_data = []
    embedding_data = []
    embeddings_reference = []
    titles = []
    abstracts = []
    title_embeddings = []
    abstract_embeddings = []

    failed_articles = []

    start_time = time.time()
    print(u'\u2500' * 30 + f"|{Path(file_path).stem}|" + u'\u2500' * 30)
    # Read through jsonl file
    with open(file_path, 'r') as file:
        for line in file:
            article_data = json.loads(line)

            # Using PMC as ID so need to make sure it exists.
            if not "pmc" in article_data["metadata"]["article_ids"]:
                failed_articles.append(article_data["filename"])
                continue

            # Create datetime format for publication dates
            dates = []
            ppub = None
            epub = None
            for date_key in article_data["metadata"]["publication_dates"]:
                date_string = article_data["metadata"]["publication_dates"][date_key]
                date_obj = datetime.strptime("-".join([(part if part.isdigit() else "01") for part in date_string.split("-")]), "%Y-%m-%d")
                if date_key == 'ppub':
                    ppub = date_obj
                elif date_key == 'epub':
                    epub = date_obj
                else:
                    dates.append(date_obj)

            tmp = (
                article_data["metadata"]["article_ids"]["pmc"],
                article_data["metadata"]["article_title"] or "",
                article_data['text_content']['abstract'] or "",
                article_data["metadata"]["journal_title"] or "",
                article_data["metadata"]["article_type"] or "",
                #SEPERATOR.join([article_data["metadata"]["article_ids"][id] for id in article_data["metadata"]["article_ids"] if id != "pmc"] or [' ']),
                #SEPERATOR.join(article_data["metadata"]["authors"] or [' ']),
                #SEPERATOR.join([aff for aff in article_data["metadata"]["affiliations"].values()] or [' ']),
                [article_data["metadata"]["article_ids"][id] for id in article_data["metadata"]["article_ids"]] or [' '],
                article_data["metadata"]["authors"] or [' '],
                [aff for aff in article_data["metadata"]["affiliations"].values()] or [' '],
                dates,
                ppub,
                epub,
            )

            # Add pmc and title/abstract to list to later calculate embeddings
            embeddings_reference.append(tmp[0])
            titles.append(tmp[1])
            if (len(tmp[2]) < 10):
                if article_data['text_content']['article'] and len(article_data['text_content']['article']) > 10:
                    #abstracts.append(article_data['text_content']['article'])
                    article = '. '.join(article_data['text_content']['article'].split('.')[:15] + [''])
                    tmp = tmp[:2] + (article_data['text_content']['article'],) + tmp[3:]
                else:
                    tmp = tmp[:2] + (article_data["metadata"]["article_title"],) + tmp[3:]

            abstracts.append(tmp[2])

            doc_data.append(tmp)
            #title_embeddings.append(article_data["embeddings"]["title"])
            #abstract_embeddings.append(article_data["embeddings"]["abstract"])

    # Done reading file
    #pg_insert_batch(cursor, TABLE_ARTICLES, doc_data)
    embedd_time_start = time.time()
    title_embeddings = get_embeddings(titles)
    abstract_embeddings = get_embeddings(abstracts)
    all_embeddings = []
    for idx, pmc in enumerate(embeddings_reference):
        # Add Title embedding
        '''
        tmp1 = (
            pmc,
            "title",
            title_embeddings[idx]
        )
        embedding_data.append(tmp1)
        tmp2 = (
            pmc,
            "abstract",
            abstract_embeddings[idx]
        )
        # Add Abstract embedding
        embedding_data.append(tmp2)
        '''
        all_embeddings.append((title_embeddings[idx], abstract_embeddings[idx]))
        
    embedd_time_stop = time.time()
    print(f"Embedded {len(embeddings_reference)} articles in {embedd_time_stop-embedd_time_start:.2f} seconds.")
    # Insert Embeddings into db
    #pg_insert_batch(cursor, TABLE_EMBEDDINGS, embedding_data)
    all_data = doc_data
    all_data = [d + all_embeddings[idx] for idx, d in enumerate(all_data)]
    pg_insert_batch(cursor, TABLE, all_data)
    end_time = time.time()
    print(f"|Adding all data from file to database took {end_time-start_time:.2f} seconds|")
    print(f"!!!Failed to insert following documents: {failed_articles}!!!")


def pg_insert_batch(cursor, table_name, doc_data):
    '''
    Insert Batch of documents into database. Prints time and document count.
    '''
    start_time = time.time()
    placeholders = ', '.join(['%s'] * len(doc_data[0]))

    #sql_string = f"""INSERT INTO {table_name} VALUES ({placeholders})"""
    #cursor.executemany(sql_string, doc_data)

    args_str = ', '.join(cursor.mogrify(f"({placeholders})", tup).decode('utf-8') for tup in doc_data)
    cursor.execute(f"INSERT INTO {table_name} VALUES " + args_str)

    end_time = time.time()
    print(f"{len(doc_data)} documents have been inserted into collection: '{table_name}' in {end_time - start_time:.2f} seconds.")


def create_search_indices(cursor):
    start_time = time.time() 
    index_queries = [ 
        f" CREATE INDEX ON {TABLE} USING hnsw (title_vector vector_cosine_ops) ",
        f" CREATE INDEX ON {TABLE} USING hnsw (abstract_vector vector_cosine_ops) ",
        #f" CREATE INDEX idx_title_content ON {TABLE} USING gin(to_tsvector('english', title)); ",
        #f" CREATE INDEX idx_content ON {TABLE} USING gin(to_tsvector('english', abstract)); ",
        #f" CREATE INDEX idx_authors_str ON {TABLE} USING gin(to_tsvector('english', authors_str)); ",
        #f" CREATE INDEX idx_affiliations_str ON {TABLE} USING gin(to_tsvector('english', affiliations_str)); ",
        #f" CREATE INDEX idx_publication_dates_str ON {TABLE} USING gin(to_tsvector('english', publication_dates_str)); ",
        #f"CREATE INDEX idx_article_ids_str ON {TABLE} USING gin(to_tsvector('english', article_ids_str)); ",
        #f" CREATE INDEX idx_journal_title ON {TABLE} USING gin(to_tsvector('english', journal_title)); ", 
        #f" CREATE INDEX idx_article_type ON {TABLE} USING gin(to_tsvector('english', article_type)); ", 
    ] 
    # Execute each index creation statement 
    for query in index_queries: 
        cursor.execute(query) 
        print(f"Index created with query: {query.strip()}")

    end_time = time.time()
    print(f"Added Search Indices to table: {TABLE} in {end_time-start_time:.2f} seconds.")



def get_embeddings(arr):
    '''
    Calculating the embeddings for the given Strings using a sentencetransformer model.

    Parameters:
    - arr (List<String>): A List/Array of Strings to be encoded.

    Returns:
    - Embeddings (List): Either a list of embeddings or a List of a single embedding.
    '''
    output = model.encode(arr)
    return output.tolist()


def search_db(cursor, query):
    '''
    Embedd the query array and take first arg as query string.
    Perform vector-search on the database (and later maybe keyword search and combine with defined weight).
    Return the entries of the top k articles with their respective score (showing impact of both keyword match score and other score later).
    '''
    start_time = time.time()
    query_embedding = get_embeddings(query)[0]
    print("Start Querying for Query ", query[0])
    # FOR SOME REASON THE SIMILARITY SCORE IS 1 - cosine sim. I invert this back to work as originially intended
    sql_string = f"""
    WITH top_articles AS (
        SELECT pmc,
        1 - (title_vector <=> '{query_embedding}') AS title_score,
        1 - (abstract_vector <=> '{query_embedding}') AS abstract_score,
        title
        FROM {TABLE}
    )
    SELECT pmc,
        GREATEST(title_score, abstract_score) AS max_similarity_score,
        CASE
            WHEN title_score >= abstract_score
            THEN 'title'
            ELSE 'abstract'
        END AS embedding_type,
        title
    FROM top_articles
    ORDER BY max_similarity_score DESC
    LIMIT {LIMIT};
    """
    cursor.execute(sql_string)
    results = cursor.fetchall()
    end_time = time.time()
    print(f"Query took {end_time-start_time:.2f} seconds!")

    # Displaying the results
    for row in results:
        print(row)
    return results


def search_with_dict(cursor, filter):
    '''
    Embedd the query array and take first arg as query string.
    Perform vector-search on the database (and later maybe keyword search and combine with defined weight).
    Return the entries of the top k articles with their respective score (showing impact of both keyword match score and other score later).
    '''
    start_time = time.time()
    print(filter['query'])
    query_embedding = get_embeddings(filter['query'])[0]
    print("Start Querying for Query ", filter['query'][0])
    # FOR SOME REASON THE SIMILARITY SCORE IS 1 - cosine sim. I invert this back to work as originially intended
    sql_string = f"""
    WITH top_articles AS (
        SELECT pmc, title, article_type, article_ids, authors, affiliations, journal_title,
        1 - (title_vector <=> '{query_embedding}') AS title_score,
        1 - (abstract_vector <=> '{query_embedding}') AS abstract_score
        FROM {TABLE}
        WHERE TRUE
    """
    '''
    if filter['article_types'] :
        sql_string += " AND article_type = ANY (%(article_types)s)"
    # TODO
    if filter['start_date'] and filter['end_date']:
        sql_string += " AND (ppub_date, epub_date) OVERLAPS (%(start_date)s, %(end_date)s))"
    '''
    sql_string += f"""
    )
    SELECT subquery.*
    FROM (
    SELECT pmc, title, article_type, article_ids, authors, journal_title,
        GREATEST(title_score, abstract_score) """
    if filter['article_ids']:
        sql_string += " + CASE WHEN CAST(%(article_ids)s AS VARCHAR) = ANY(article_ids) THEN 1 ELSE 0 END"
    if filter['fts']:
        sql_string += """
            + ( 0.3 * similarity( CAST(%(fts)s AS TEXT) , journal_title))
            + ( 0.3 * arr_similarities( CAST(%(fts)s AS TEXT) , affiliations))
            + ( 0.3 * arr_similarities( CAST(%(fts)s AS TEXT) , authors))
            """
    sql_string += f"""
    AS max_similarity_score,
        CASE
            WHEN title_score >= abstract_score
            THEN 'title'
            ELSE 'abstract'
        END AS embedding_type
    FROM top_articles) subquery
    ORDER BY max_similarity_score DESC
    LIMIT {LIMIT};
    """
    #print(sql_string)
    cursor.execute(sql_string, filter)
    results = cursor.fetchall()
    end_time = time.time()
    print(f"Query took {end_time-start_time:.2f} seconds!")

    # Displaying the results
    for row in results:
        print(row)
    return results

def test_fts(cursor, query):
    start_time = time.time()
    print("Start Querying for Query ", query[0])

#        (0.2 * ts_rank(to_tsvector('english', d.abstract), to_tsquery('english', st.term))) +
#        OR to_tsvector('english', d.abstract) @@ to_tsquery('english', st.term)
#        (0.1 * ts_rank(to_tsvector('english', array_to_string(d.publication_dates::TEXT[], ' ')), to_tsquery('english', st.term))) +
#        OR to_tsvector('english', array_to_string(d.publication_dates::TEXT[], ' ')) @@ to_tsquery('english', st.term)
#       string_to_array(d.authors, '{SEPERATOR}'),
#        (0.2 * ts_rank(to_tsvector('english', d.authors), to_tsquery('english', st.term))) +
#        OR to_tsvector('english', d.authors) @@ to_tsquery('english', st.term)

#   string_to_array(d.authors, '{SEPERATOR}'), string_to_array(d.affiliations, '{SEPERATOR}'), d.publication_dates, 
 #       d.title, d.journal_title, d.article_type, string_to_array(d.article_ids, '{SEPERATOR}'),


    sql_string = f"""
    WITH search_terms AS (
        SELECT unnest(string_to_array(%s, ' ')) AS term
    )
    SELECT DISTINCT d.pmc, d.title, d.journal_title, string_to_array(d.authors, '{SEPERATOR}'),
        string_to_array(d.affiliations, '{SEPERATOR}'),
        (1 * ts_rank(to_tsvector('english', d.authors), to_tsquery('english', st.term))) +
        (1 * ts_rank(to_tsvector('english', d.affiliations), to_tsquery('english', st.term))) +
        (1 * ts_rank(to_tsvector('english', d.journal_title), to_tsquery('english', st.term))) +
        (1 * ts_rank(to_tsvector('english', d.article_type), to_tsquery('english', st.term))) +
        (1 * ts_rank(to_tsvector('english', d.article_ids), to_tsquery('english', st.term))) AS combined_rank
    FROM {TABLE} d
    JOIN search_terms st ON (
        to_tsvector('english', d.article_ids) @@ to_tsquery('english', st.term)
        OR to_tsvector('english', d.authors) @@ to_tsquery('english', st.term)
        OR to_tsvector('english', d.affiliations) @@ to_tsquery('english', st.term)
        OR to_tsvector('english', d.journal_title) @@ to_tsquery('english', st.term)
        OR to_tsvector('english', d.article_type) @@ to_tsquery('english', st.term)
    )
    ORDER BY combined_rank DESC
    LIMIT {LIMIT};
    """

    cursor.execute(sql_string, (query[0],))
    results = cursor.fetchall()
    end_time = time.time()
    print(f"Query took {end_time-start_time:.2f} seconds!")

    # Displaying the results
    for row in results:
        print(row)
    return results


def fuzzy_text_search(cursor, query):
    # Fuzzy text search using trigrams - Usefull when spelling might not be exact
    start_time = time.time()
    print("Start Querying for Query ", query[0])
    sql_string = f"""
    SELECT pmc, title, journal_title, string_to_array(authors, '{SEPERATOR}'),
        string_to_array(affiliations, '{SEPERATOR}'),
        (1 * strict_word_similarity(authors, %s)) +
        (1 * word_similarity(affiliations, %s)) +
        (1 * similarity(journal_title, %s))
        AS similarity
    FROM {TABLE}
    ORDER BY similarity DESC
    LIMIT {LIMIT};
    """

    cursor.execute(sql_string, (query[0], query[0], query[0]))
    results = cursor.fetchall()
    end_time = time.time()
    print(f"Query took {end_time-start_time:.2f} seconds!")

    # Displaying the results
    for row in results:
        print(row)
    return results


def hybrid_search(cursor, query):
    # Combine semantic search and keyword search (for now fuzzy search over 2 fields)
    start_time = time.time()
    query_embedding = get_embeddings(query)[0]
    k = 60
    print("Start Querying for Query ", query[0])


    sql_string = f"""
    WITH semantic_search AS (
        WITH top_articles AS (
            SELECT pmc, title,
            1 - (title_vector <=> '{query_embedding}') AS title_score,
            1 - (abstract_vector <=> '{query_embedding}') AS abstract_score
            FROM {TABLE}
        )
        SELECT pmc,
            GREATEST(title_score, abstract_score) AS similarity_score,
            CASE
                WHEN title_score >= abstract_score
                THEN 'title'
                ELSE 'abstract'
            END AS embedding_type,
            title
        FROM top_articles
        ORDER BY similarity_score DESC
        LIMIT {PRE_LIMIT}
    ),
    keyword_search AS (
            SELECT pmc, title,
                string_to_array(affiliations, '{SEPERATOR}'),
                (1 * strict_word_similarity(authors, %(query)s)) +
                (1 * word_similarity(affiliations, %(query)s)) +
                (1 * similarity(journal_title, %(query)s))
                AS similarity_score
            FROM {TABLE}
            ORDER BY similarity_score DESC
            LIMIT {PRE_LIMIT}
    )
    SELECT
        COALESCE(semantic_search.pmc, keyword_search.pmc) AS pmc,
        COALESCE(semantic_search.title, keyword_search.title) AS title,
        COALESCE(semantic_search.similarity_score, 0.0) AS semantic_score,
        COALESCE(keyword_search.similarity_score, 0.0) AS keyword_score,
        COALESCE({SEMANTIC_WEIGHT} * semantic_search.similarity_score, 0.0) + 
        COALESCE((1.0 - {SEMANTIC_WEIGHT}) * keyword_search.similarity_score, 0.0) AS score
    FROM semantic_search
    FULL OUTER JOIN keyword_search ON semantic_search.pmc = keyword_search.pmc
    ORDER BY score DESC
    LIMIT {LIMIT}
    """

    cursor.execute(sql_string, {'query': query[0]})
    columns = list(cursor.description)
    results = cursor.fetchall()
    end_time = time.time()
    print(f"Query took {end_time-start_time:.2f} seconds!")
    for row in results:
        print(f"PMC: {row[0]:12} | TITLE: {row[1]:150} | SEMANTIC_SCORE: {row[-3]:.4f} | KEYWORD_SCORE: {row[-2]:.4f} || SCORE: {row[-1]:.4f}")
    dict_res = [{col.name: row[i] for i, col in enumerate(columns)} for row in results]
    print(dict_res)
    return dict_res