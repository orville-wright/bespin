#! python3
from requests_html import HTMLSession
import logging
import argparse
import dotenv
import os
import pandas as pd
#import modin.pandas as pd
from rich import print

from neo4j import GraphDatabase, RoutingControl



# ML / NLP section ################### Class
class neo4j_auradb:
    """
    Class to Graph Database operations
    """

    # global accessors
    URI = None           # neo4j AURA free instance connection URL (laoded from .env)
    AUTH = None          # neo4j AURA free instance auth credential (loaded from .env)
    args = []            # class dict to hold global args being passed in from main() methods
    driver = None        # driver instance
    instance = None      #  Neo4j databse instacne name : i.e. used in database="name"
    yfn = None           # Yahoo Finance News reader instance
    graph_df0 = None     # Pandas Data Frame
    yti = None           # unique instance identifier
    cycle = 0            # class thread loop counter

    def __init__(self, yti, global_args):
        cmi_debug = __name__+"::"+self.__init__.__name__
        logging.info( f'%s - Instantiate.#{yti}' % cmi_debug )

        self.args = global_args                            # Only set once per INIT. all methods are set globally
        self.yti = yti
        load_status = dotenv.load_dotenv()
        if load_status is False:
            raise RuntimeError('Environment variables not loaded.')
        else:
            # Retrieve Neo4j Aura credentials from .env file variables
            self.URI = os.getenv("NEO4J_URI")
            USERNAME = os.getenv("NEO4J_USERNAME")
            PASSWORD = os.getenv("NEO4J_PASSWORD")
            INSTANCE = os.getenv("NEO4J_DATABASE")
            self.AUTH = (USERNAME, PASSWORD)
            self.instance = INSTANCE        # WARNING: Free Neo4j AURA doesn't allow multiple named instances. "neo4j" only!

# ########################### 1
    def con_neo4j_auradb(self, _yti):
        """
        Connect to the Neo4j AURA KnowledgeGraph DB (Free limited web service)
        """
        cmi_debug = __name__+"::"+self.con_neo4j_auradb.__name__+".#"+str(_yti)
        logging.info( f"%s - IN instance: {self.instance}" % cmi_debug )
        # URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
        # BAD code - with GraphDatabase.driver(self.URI, auth=self.AUTH) as _driver:    
        try:
            _driver = GraphDatabase.driver(self.URI, auth=self.AUTH)
            _driver.verify_connectivity()
            self.driver = _driver       # cache driver object handle inside class instance
            logging.info( f'%s - {self.driver} connection verified !' % cmi_debug )
            return self.driver
        except Exception as e:
            print (f"Neo4j AURA DB connection failed: {e}")
            self.driver = None
            return None

# ###########################  2
    def close_neo4j_auradb(self, _yti, _driver):
        """
        Close our connection to the Neo4j AURA KnowledgeGraph DB (Free limited web service)
        """
        cmi_debug = __name__+"::"+self.close_neo4j_auradb.__name__+".#"+str(_yti)
        logging.info( f"%s - Working DB: {_driver} / Class DB: {self.driver}" % cmi_debug )
        #_driver = GraphDatabase.driver(self.URI, auth=self.AUTH)
        #session = self.driver.session()
        self.driver.close()
        _driver.close()
        return

# ###########################  3
    def check_node_exists(self, _yti, ticker_symbol):
        """
        Create a Graph NODE
        Assumes driver has been successfully created and saved to self.driver
        node_data_package = dict of data we want created in GraphDB
        """
        # query = """
        # MATCH (s:Symbol {symbol: $symbol})
        # RETURN s.id IS NOT NULL AS present
        # """
        symbol = ticker_symbol.upper()
        cmi_debug = __name__+"::"+self.check_node_exists.__name__+".#"+str(_yti)
        logging.info( f'%s - Check {self.driver} for Symbol [ {symbol} ]' % cmi_debug )
        try:
            with self.driver.session() as session:
                query = """
                MATCH (s:Symbol {symbol: $symbol})
                RETURN count(s) > 0 AS present
                """
                result = session.run(query, symbol=symbol)     # Result object
                record = result.single()            # class 'neo4j._data.Record'> output... <Record present=False>
                return record["present"]            # will return 'None' if nothing found
        except Exception as e:
            logging.error( f"%s - Exception checking node: {e}" % cmi_debug )
            return 99

# ###########################  4
    def create_sym_node(self, ticker_symbol, df_final, sen_report, sen_metrics, sen_2v_metrics):
        """
        Create a Stock Symbol Graph NODE 
        - with enhanced computed Qunat sentiment attributes
        """
        symbol = ticker_symbol.upper()
        cmi_debug = __name__+"::"+self.create_sym_node.__name__+".#"+str(self.yti)
        logging.info( f'%s - Creating graph node for symbol: [ {ticker_symbol} ]...' % cmi_debug )
        #print ( f"DEBUG-#120: sen_df:{sentiment_df}" )
        with self.driver.session() as session:
            if sen_report and sen_metrics and sen_2v_metrics and not df_final.empty:   # all data structs contian data
                # Define sentiment elements we want in the NODE graph
                df_row = df_final.iloc[-1]  # Get the last row of the DataFrame for sentiment metrics
                query = (
                    "CREATE (s:Symbol {"
                    "symbol: $symbol, "
                    "id: $symbol, "
                    "uid: randomUUID(), "
                    "sentiment: $sentiment, "
                    "sent_bias: $sent_bias, "
                    "sent_base: $sent_base, "
                    "sent_progress: $sent_progress, "
                    "conviction: $conviction, "
                    "positivity: $positivity, "
                    "negativity: $negativity, "
                    "neutrality: $neutrality, "
                    "signal_mass_pos: $signal_mass_pos, "
                    "signal_mass_neg: $signal_mass_neg, "
                    "signal_mass_neu: $signal_mass_neu, "
                    "pos_mean: $pos_mean, "
                    "neg_mean: $neg_mean, "
                    "neu_mean: $neu_mean "
                    "}) RETURN s.uid AS node_id"
                )
                result = session.run(query, 
                    symbol=symbol,
                    id=symbol,
                    sentiment=sen_report.get('sentiment'),
                    sent_bias=sen_2v_metrics.get('sentiment'),
                    sent_base=sen_report.get('base_sentiment'),
                    sent_progress=sen_report.get('band_progress'),
                    conviction=sen_2v_metrics.get('conviction'),
                    positivity=sen_report.get('positive_share'),
                    negativity=sen_report.get('negative_share'),
                    neutrality=sen_report.get('neutral_share'), 
                    signal_mass_pos=sen_metrics.get('positive_strength'),
                    signal_mass_neg=sen_metrics.get('negative_strength'),
                    signal_mass_neu=sen_metrics.get('neutral_strength'),
                    pos_mean=float(df_row['psnt']),
                    neg_mean=float(df_row['nsnt']),
                    neu_mean=float(df_row['zsnt'])
                )
            else:
                # Fallback to basic symbol node if no sentiment data
                query = ("CREATE (s:Symbol {symbol: $symbol, id: $symbol}) "
                         "RETURN s.id AS node_id")
                result = session.run(query, symbol=symbol)
            
            record = result.single()
            return record["node_id"]

# ###########################  5
    def dump_symbols(self, yti):
        """
        Create a Graph NODE
        Assumes driver has been successfully created and saved to self.driver
        node_data_package = dict of data we want created in GraphDB
        """
        cmi_debug = __name__+"::"+self.dump_symbols.__name__+".#"+str(self.yti)
        logging.info( f'%s - Runing Query to dump Sumbols list ]...' % cmi_debug )

        # a Graph node looks like this:
        # its a class of:  neo4j._data.Record
        # <Node element_id='4:174744d5-22cc-4690-90f0-47f1bc98fd53:5' labels=frozenset({'Symbol'}) properties={'symbol': 'nvda', 'id': '6563b467-685e-4520-b4a6-15bfdeeb8812'}>
        # n.data = {'s': {'symbol': 'pfe', 'id': '8df7d4f3-a74a-4a9d-930c-83191bdb88d5'}}

        print ( f"Node symbols in Graph...")
        with self.driver.session() as session:
            query = ("MATCH ( s:Symbol ) "
                     "RETURN s")
            result = session.run(query)     # Result object
            scount = 1
            buffer = result.fetch(500)      # pull 500 enteries into the buffer
            print ( f"Results BUFFER has [ {len(buffer)} ] elements\n")
            '''
            for i in buffer:                # working on: neo4j._data.Record 
                print ( f"ITEM: {scount} : \t SYMBOL found: {i['s']._properties['symbol']} \t ID: {i['s']._properties['id']}" )
                print ( f"========================================================================================" )
                scount += 1
            '''
            rec_done = result.consume()       # ResultSummary objects
            return rec_done

# ###########################  6
    def create_article_nodes(self, df_final, symbol):
        """
        Create Article Graph NODEs from df_final dataframe using APOC for dynamic labels
        Assumes driver has been successfully created and saved to self.driver
        df_final = dataframe containing article sentiment data
        Creates nodes with static "Article" label and dynamic label based on urlhash
        Checks for existing nodes with Hash_{urlhash} label before creating
        """
        cmi_debug = __name__+"::"+self.create_article_nodes.__name__+".#"+str(self.yti)
        logging.info( f'%s - Creating article nodes from DF...' % cmi_debug )
        symbol = symbol.upper()

        created_nodes = []
        skipped_nodes = []
        
        with self.driver.session() as session:
            for idx, row in df_final.iterrows():
                # Skip the totals row
                if row['art'] == 'Totals' or pd.isna(row['urlhash']) or row['urlhash'] == '':
                    continue
                
                # Prefix urlhash with 'Hash_' to make it a valid Neo4j label
                dynamic_label = f"Hash_{str(row['urlhash'])}"
                
                # Check if article node with this urlhash already exists
                # Simply check for Article nodes with matching urlhash property
                check_query = "MATCH (n:Article {urlhash: $urlhash}) RETURN n.id AS existing_id LIMIT 1"
                check_result = session.run(check_query, urlhash=str(row['urlhash']))
                existing_record = check_result.single()
                
                if existing_record:
                    # Node already exists, skip creation
                    skipped_nodes.append((existing_record["existing_id"], str(row['urlhash'])))
                    # logging.info( f'%s - Article node with label {dynamic_label} already exists, skipping creation for urlhash: {row["urlhash"]}' % cmi_debug )
                    continue
                
                # Node doesn't exist, create it using APOC
                create_query = (
                    "CALL apoc.create.node([$static_label, $dynamic_label], {"
                    "urlhash: $urlhash, "
                    "id: randomUUID(), "
                    "usedby: $usedby, "
                    "art: $art, "
                    "positive: $positive, "
                    "neutral: $neutral, "
                    "negative: $negative, "
                    "psnt: $psnt, "
                    "nsnt: $nsnt, "
                    "zsnt: $zsnt"
                    "}) YIELD node RETURN node.id AS node_id"
                )
                
                result = session.run(create_query,
                    static_label="Article",
                    dynamic_label=dynamic_label,
                    urlhash=str(row['urlhash']),
                    art=int(row['art']),
                    usedby=symbol,
                    positive=float(row['positive']),
                    neutral=float(row['neutral']),
                    negative=float(row['negative']),
                    psnt=float(row['psnt']),
                    nsnt=float(row['nsnt']),
                    zsnt=float(row['zsnt'])
                )
                
                record = result.single()
                created_nodes.append((record["node_id"]))
                # logging.info( f'%s - Created article node with labels [Article, {dynamic_label}]: {record["node_id"]} for urlhash: {row["urlhash"]}' % cmi_debug )
        
        logging.info( f'%s - Summary: {len(created_nodes)} nodes created, {len(skipped_nodes)} nodes skipped (already existed)' % cmi_debug )
        return created_nodes     # Returns a list of tuples (node_id, urlhash) for created nodes

# ###########################  7
    def create_sym_art_rels(self, ticker_symbol, df_final, agency="Unknown", author="Unknown", published="Unknown", article_teaser="Unknown"):
        """
        Create HAS_ARTICLE relationships between Symbol and Article nodes
        ticker_symbol = the stock symbol
        df_final = dataframe containing article data
        Relationship properties: agency, author, published, article_teaser can be provided
        Checks for existing relationships before creating to avoid duplicates
        """
        symbol = ticker_symbol.upper()
        cmi_debug = __name__+"::"+self.create_sym_art_rels.__name__+".#"+str(self.yti)
        # logging.info( f'%s - Creating HAS_ARTICLE relationships for symbol: [ {symbol} ]...' % cmi_debug )

        created_relationships = []
        skipped_relationships = []
        
        with self.driver.session() as session:
            for idx, row in df_final.iterrows():
                # Skip the totals row
                if row['art'] == 'Totals' or pd.isna(row['urlhash']) or row['urlhash'] == '':
                    continue
                
                # Prefix urlhash with 'Hash_' to match the dynamic label from create_article_nodes
                dynamic_label = f"Hash_{str(row['urlhash'])}"
                
                # Check if relationship already exists
                check_query = (
                    "MATCH (s:Symbol {symbol: $symbol}) "
                    "MATCH (a:Article {urlhash: $urlhash}) "
                    "MATCH (s)-[r:HAS_ARTICLE]->(a) "
                    "RETURN r LIMIT 1"
                )
                
                check_result = session.run(check_query,
                    symbol=symbol,
                    urlhash=str(row['urlhash'])
                )
                existing_rel = check_result.single()
                
                if existing_rel:
                    # Relationship already exists, skip creation
                    skipped_relationships.append(str(row['urlhash']))
                    # logging.info( f'%s - REL already exists: {symbol} - {row["urlhash"]}, skipping...' % cmi_debug )
                    continue
                
                # Relationship doesn't exist, create it
                create_query = (
                    "MATCH (s:Symbol {symbol: $symbol}) "
                    f"MATCH (a:{dynamic_label} {{urlhash: $urlhash}}) "
                    "CREATE (s)-[r:HAS_ARTICLE {"
                    "art: $art, "
                    "locality: $locality, "
                    "syndicatedby: $syndicatedby, "
                    "news_agency: $news_agency, "
                    "author: $author, "
                    "published: $published, "
                    "article_teaser: $article_teaser, "
                    "urlhash: $urlhash"
                    "}]->(a) "
                    "RETURN r"
                )
                
                result = session.run(create_query,
                    symbol=symbol,
                    urlhash=str(row['urlhash']),
                    art=int(row['art']),
                    locality="Local",
                    syndicatedby=(ticker_symbol.upper()),
                    news_agency=agency,
                    author=author,
                    published=published,
                    article_teaser=article_teaser
                )
                
                record = result.single()
                '''
                if record:
                    created_relationships.append(str(row['urlhash']))
                    logging.info( f'%s - Created HAS_ARTICLE relationship for urlhash: {row["urlhash"]}' % cmi_debug )
                else:
                    logging.warning( f'%s - REL Create FAIL urlhash: {row["urlhash"]}' % cmi_debug )
                '''
                
        logging.info( f'%s - Summary: {len(created_relationships)} relationships created, {len(skipped_relationships)} relationships skipped (already existed)' % cmi_debug )
        return created_relationships

# ###########################  8
    def news_agency(self):
        """
        Create YahooFinance NewsAgency node and establish STOCK_NEWS relationships with all Symbol nodes
        Checks for existing YahooFinance node and relationships before creating
        Direction: YahooFinance -[STOCK_NEWS]-> Symbol
        """
        cmi_debug = __name__+"::"+self.news_agency.__name__+".#"+str(self.yti)
        logging.info( f'%s - Creating YahooFinance NewsAgency node and STOCK_NEWS relationships...' % cmi_debug )

        created_relationships = []
        skipped_relationships = []
        yahoo_node_created = False
        
        with self.driver.session() as session:
            # Check if YahooFinance node already exists
            check_yahoo_query = "MATCH (y:YahooFinance) RETURN y.id AS existing_id LIMIT 1"
            check_result = session.run(check_yahoo_query)
            existing_yahoo = check_result.single()
            
            if existing_yahoo:
                logging.info( f'%s - YahooFinance node exists, skipping...' % cmi_debug )
            else:
                # Create YahooFinance node
                create_yahoo_query = (
                    "CREATE (y:YahooFinance {"
                    "NewsAgency: 'YahooFinance', "
                    "id: 'YahooFinance', "
                    "uid: randomUUID()"
                    "}) RETURN y.uid AS node_id"
                )
                yahoo_result = session.run(create_yahoo_query)
                yahoo_record = yahoo_result.single()
                yahoo_node_created = True
                logging.info( f'%s - Created YahooFinance node: {yahoo_record["node_id"]}' % cmi_debug )
            
            # Get all Symbol nodes
            symbols_query = "MATCH (s:Symbol) RETURN s.symbol AS symbol"
            symbols_result = session.run(symbols_query)
            symbols = [record["symbol"] for record in symbols_result]
            
            logging.info( f'%s - Found {len(symbols)} Symbol nodes to process' % cmi_debug )
            
            # Process each Symbol node
            for symbol in symbols:
                # Check if STOCK_NEWS relationship already exists
                check_rel_query = (
                    "MATCH (y:YahooFinance) "
                    "MATCH (s:Symbol {symbol: $symbol}) "
                    "MATCH (y)-[r:STOCK_NEWS]->(s) "
                    "RETURN r LIMIT 1"
                )
                
                check_rel_result = session.run(check_rel_query, symbol=symbol)
                existing_rel = check_rel_result.single()
                
                if existing_rel:            # Relationship already exists, skip creation
                    skipped_relationships.append(symbol)
                    # logging.info( f'%s - STOCK_NEWS rel exists for symbol: {symbol}, skipping' % cmi_debug )
                    continue
                
                # Create STOCK_NEWS relationship: YahooFinance -> Symbol
                create_rel_query = (
                    "MATCH (y:YahooFinance) "
                    "MATCH (s:Symbol {symbol: $symbol}) "
                    "CREATE (y)-[r:STOCK_NEWS {"
                    "symbol: $symbol"
                    "}]->(s) "
                    "RETURN r"
                )
                
                rel_result = session.run(create_rel_query, symbol=symbol)
                rel_record = rel_result.single()

        logging.info( f'%s - YF node create: {yahoo_node_created} / Created: {len(created_relationships)} / Skipped: {len(skipped_relationships)}' % cmi_debug )
        return {
            "node_created": yahoo_node_created,
            "relationships_created": created_relationships,
            "relationships_skipped": skipped_relationships
        }

    #############################
    # scan all node type = Symbol
    # list all keys
    # checking that attributeCount = 17
    # - we assign 17 core attibutes on a successful Symbol node creation
    # = if Symbol node fails, a def 2 attributes are created. So any Symbol node with 2 needs to be tidied up (correctly re-generated)
    def check_symbol_attrs(self, symbol):
        
        with self.driver.session() as session:
                #
                #create_rel_query = ("MATCH (a:Symbol) {symbol: $symbol} RETURN size(keys(a)) AS attributeCount" )
                create_rel_query = ("MATCH (n:Symbol) where n.symbol={symbol: $symbol} RETURN size(keys(n)) AS attributeCount" )
                check_result = session.run(create_rel_query, symbol=symbol)
                check_record = check_result.single()
    
        return check_record