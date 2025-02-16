"""
Contains all functions/methods to query the database:
- query_db(conn, filter)
- ...
"""
from app.ml_models.embedding_model import get_embeddings
import datetime

TABLE = "pmcArticles"

def get_distinct_column_values(conn, column_name):
    """Query for all article types and return as list."""
    cursor = conn.cursor()
    # Check if specified column_name exists
    sql_string = """
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = %s AND column_name = %s;
    """
    cursor.execute(sql_string, (TABLE, column_name))
    results = cursor.fetchall()
    print(results)
    if results == None:
        print("Column Name does not exist")
        raise Exception
    # Query all Distinct Values here
    sql_string = f"SELECT DISTINCT {column_name} FROM {TABLE};"
    cursor.execute(sql_string)
    results = cursor.fetchall()
    cursor.close()
    list_result = [entry[0] for entry in results]
    print(list_result)
    return list_result


def query_db(conn, filter:dict):
    """
    Query the database (connection given by conn) with the filter dict.
    The dictionary has to contain: {query:str, ...}

    Parameters:
    - conn (psycopg2 connection): Connection to postgres database.
    - filter (dict): Dictionary containing filter parameters.

    Returns:
    - list(dict): [{pmc:str, title:str, ...}, ...]
    """

    cursor = conn.cursor()

    query_embedding = get_embeddings([filter['query']])[0]
    
    # FOR SOME REASON THE SIMILARITY SCORE IS 1 - cosine sim. 
    # I invert this back, so that similarity is the "normal" cosine similarity.
    sql_string = f"""
    WITH top_articles AS (
        SELECT pmc, title, abstract, journal_title, article_type,
        article_ids, authors, affiliations, publication_dates, ppub_date, epub_date,
        1 - (title_vector <=> '{query_embedding}') AS title_score,
        1 - (abstract_vector <=> '{query_embedding}') AS abstract_score
        FROM {TABLE}
        WHERE TRUE
    """
    # TODO: Add filters
    if filter['start_date'] and filter['end_date']:
        sql_string += f" AND (ppub_date, epub_date) OVERLAPS ( CAST(%(start_date)s AS DATE), CAST(%(end_date)s AS DATE) )"
    if filter['article_types']:
        print(filter['article_types'])
        sql_string += f" AND article_type = ANY( CAST(%(article_types)s AS TEXT[]) )"
    sql_string += """
    )
    SELECT subq.*
    FROM(
        SELECT pmc, title, abstract, journal_title, article_type, publication_dates,
            article_ids, authors, affiliations,
            title_score, abstract_score, ppub_date, epub_date,
            GREATEST(title_score, abstract_score)
    """
    if filter['article_ids']:
        sql_string += " + CASE WHEN CAST(%(article_ids)s AS VARCHAR) = ANY(article_ids) THEN 1 ELSE 0 END "
    if filter['fts_aff_aut_jtl']:
        sql_string += """
            + ( 0.3 * similarity( CAST(%(fts_aff_aut_jtl)s AS TEXT) , journal_title))
            + ( 0.3 * arr_similarities( CAST(%(fts_aff_aut_jtl)s AS TEXT) , affiliations))
            + ( 0.3 * arr_similarities( CAST(%(fts_aff_aut_jtl)s AS TEXT) , authors))
            """
    score_type = "title_score" if filter['score_embedding_type']=='title' else "abstract_score" if filter['score_embedding_type']=='abstract' else "max_similarity_score"
    sql_string +=  f"""AS max_similarity_score,
            CASE
                WHEN title_score >= abstract_score
                THEN 'title'
                ELSE 'abstract'
            END AS embedding_type
        FROM top_articles) subq
    WHERE {score_type} > %(similarity_threshhold)s
    ORDER BY {score_type} DESC
    """
    if filter['result_limit'] > -1:
        sql_string += "LIMIT %(result_limit)s"

    cursor.execute(sql_string + ';', filter)
    columns = list(cursor.description)
    results = cursor.fetchall()
    cursor.close()

    dict_res = [{col.name: row[i] for i, col in enumerate(columns)} for row in results]
    return dict_res