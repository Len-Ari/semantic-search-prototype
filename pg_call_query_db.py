import pg_db_setup_query as dbFunc
import time
from pg_db_connect import get_database_connection

# Need to add path to jsonl file
file_path = ''

query = ["chronic kidney disease"]


with get_database_connection() as conn:
    with conn.cursor() as cursor:
        '''  '''
        #dbFunc.create_tables(cursor)
        #dbFunc.createFunction(cursor)
        #conn.commit()
        #start_time = time.time()
        #dbFunc.add_to_db(cursor, file_path)
        #conn.commit()
        #end_time = time.time()
        #print(f"!!!Embedding directory took {end_time-start_time:.2f} seconds!!!")
        #dbFunc.create_search_indices(cursor)
        #dbFunc.search_db(cursor, query)
        #dbFunc.test_fts(cursor, query)
        #dbFunc.fuzzy_text_search(cursor, query)
        #dbFunc.hybrid_search(cursor, query)
        dbFunc.search_with_dict(cursor, {'query': query, 'article_ids': ''})
