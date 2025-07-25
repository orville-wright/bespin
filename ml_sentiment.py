#! python3
from requests_html import HTMLSession
import pandas as pd
#import modin.pandas as pd
import numpy as np
import re
import os
import sys
import logging
import argparse
from rich import print

from ml_cvbow import ml_cvbow
import nltk.data
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from transformers import pipeline

# ML / NLP section #############################################################
class ml_sentiment:
    """
    Class to manage the Global Database of NLP Sentiment data
    and provide statistical analysis of sentiment
    """

    # global accessors
    active_urlhash = None  # Current URL hash being processed
    args = []            # class dict to hold global args being passed in from main() methods
    art_buffer = []     # Buffer to hold article text for processing
    classifier = None    # NLP classidier pipeline
    cycle = 0            # class thread loop counter
    df0_row_count = 0
    mlnlp_uh = None      # URL Hinter instance
    sen_df0 = None       # sentiment for this artile ONLY (gets overwritten each time per article)
    sen_df1 = None       # uNUSED
    sen_df2 = None       # ? unknown
    sen_df3 = None       # A long lasting DF to collect all sentiment data
    sen_data = []       # Data to be added to the DataFrame
    sentiment_count = { 'positive': 0, 'negative': 0, 'neutral': 0 }  # Sentiment counts for this article
    ttc = 0             # Total Tokens generated in the scnetcne being analyzed
    twc = 0             # Total Word count in this scentence being analyzed
    yti = 0
    
    # Techcnial analysys dict defines sentiment score to description mapping
    s_categories = {
            200: (['Bullishly positive', 200]),
            100: (['Trending bullish', 100]),
            50: (['Positive', 50]),
            25: (['Trending positive', 25]),
            0: (['Neutral', 0]),
            -25: (['Trending negative', -25]),
            -50: (['Negative', -50]),
            -100: (['Somewhat Bearish', -100]),
            -200: (['Bearishly negative', -200])
            }
        
    ######################## init ##########################################
    def __init__(self, yti, global_args):
        cmi_debug = __name__+"::"+self.__init__.__name__
        logging.info( f'%s   - Instantiate.#{yti}' % cmi_debug )
        self.args = global_args                            # Only set once per INIT. all methods are set globally
        self.yti = yti
        return

##################################### 1 ####################################
    def save_sentiment(self, yti, data_set):
        """
        Save key ML sentiment info to global sentimennt Dataframe
        data_set = a dict
        """
        self.yti = yti
        cmi_debug = __name__+"::"+self.save_sentiment.__name__
        logging.info( f'%s - Save sentiment metrics to DF...' % cmi_debug )
        x = self.df0_row_count      # get last row added to DF
        x += 1

        # need to add the url hash in here, otherwise I cant do useful analysis
        sym = data_set["sym"]
        art = data_set["article"]
        urlhash = data_set["urlhash"]
        chk = data_set["chunk"]
        rnk = data_set["rank"]
        snt = data_set["sent"]

        ################################ 6 ####################################
        # now construct our list for concatinating to the dataframe 
        logging.info( f"%s ============= Data prepared for DF =============" % cmi_debug )
        # sen_package = dict(sym=symbol, article=item_idx, chunk=i, sent=sen_result['label'], rank=raw_score )
        self.sen_data = [[ \
                    x, \
                    sym, \
                    art, \
                    urlhash, \
                    chk, \
                    rnk, \
                    snt ]]
        
        self.df0_row = pd.DataFrame(self.sen_data, columns=[ 'Row', 'Symbol', 'art', 'urlhash', 'chk', 'rnk', 'snt' ], index=[x] )
        self.sen_df0 = pd.concat([self.sen_df0, self.df0_row])

        self.df0_row_count = x

        return

##################################### 2 ####################################
    def compute_sentiment(self, symbol, item_idx, scentxt, urlhash, ext):
        """
        called by:  extract_article_data -> compute_sentiment(symbol, item_idx, local_stub_news_p, hs, 0)
        
        Tokenize and compute scentcen chunk sentiment
        scentxtx = BS4 all <p> zones that look/feel like scentence/paragraph text
        WARN: scentxt is a list of BS4 html <p> elements, NOT the raw text. It must be treated as a html data row.
              crawl4ai extarcts the buulk raw text in 1 list[] and discards the HTML <p> tags.
              crawl4ai text must be chunked @ model truncation length, i.e.  tokenizer_mml
        """
        #if self.args['bool_verbose'] is True:        # Logging level
        cmi_debug = __name__+"::"+self.compute_sentiment.__name__+".#"+str(self.yti)
        self.item_idx = item_idx
        self.yti = item_idx
        self.ext_type = ext
        logging.info( f'%s - Init NLP Tokenizor, Vectorizer & Stopwords engine.#{self.ext_type}...' % cmi_debug )
        self.vectorz = ml_cvbow(item_idx, self.args)   
        self.stop_words = stopwords.words('english')
        #classifier = pipeline('sentiment-analysis')
        logging.info( f'%s - Init HF classifier model pipeline: mrm8488/distilroberta...' % cmi_debug )
        # this is the real AI model LLM computation. GPU goes brrrr....!!
        self.classifier = pipeline(task="sentiment-analysis", model="mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis")
        self.tokenizer_mml = self.classifier.tokenizer.model_max_length
        self.ttc = 0
        self.twc = 0
        self.sentiment_count["positive"] = 0
        self.sentiment_count["negative"] = 0
        self.sentiment_count["neutral"] = 0
        self.active_urlhash = urlhash
        if self.ext_type == 0:
            logging.info( f"%s - BS4 engine.#0 - Transformer truncation: {self.tokenizer_mml}" % cmi_debug )
            # try:
            for i in range(0, len(scentxt)):
                logging.info( f"%s - Eval TEXT char length: {len(scentxt[i].text)} chars" % cmi_debug )
                print (f"##### DEBUG:\n{scentxt[i].text}") 
                truncated = "Undef"
                if len(scentxt[i].text) >= 100: # self.tokenizer_mml:    # only chunk into blocklets on truncation altert
                    truncated = "Trctd!"
                    scentxt_d = dict()                                # init an empty dict
                    scentxt_d["0"] = scentxt[i].text
                    blocket_d = self.c4_chunker(scentxt_d, 100)   # TESTING DEBIGGING !!!! result = {} of blocklets
                    #blocket_d = self.c4_chunker(std, self.tokenizer_mml)   # result = {} of blocklets
                    for scentxt_k, scentxt_d in blocket_d.items():
                        logging.info( f"%s - Classification cycle: {scentxt_k}" % cmi_debug )
                        self.ttc, self.twc, cr = self.dict_processor(symbol, blocket_d)    # Exec AI NLP classifier inside dict_processor() !!
                else:
                    logging.info( f"%s - No truncation: Short p TEXT " % cmi_debug )
                    breakpoint()
            return self.ttc, self.twc, cr
            # except:
            #    logging.info( f"%s - ERROR: Compute sent BS4 pre-processor" % cmi_debug )
            #return 0, 0, 0
        else:
            logging.info( f"%s - BS4 engine.#1 - Transformer truncation: {self.tokenizer_mml} / ({len(scentxt[i].text)})" % cmi_debug )
            for i in range(0, len(scentxt)):
                print (f"##### DEBUG:\n{scentxt[i].text}") 
                logging.info( f"%s - Eval TEXT char length: {len(scentxt[i].text)}" % cmi_debug )   # cycle through all scentenses/paragraphs sent to us
                truncated = "Undef"
                if len(scentxt[i].text) > 100: # self.tokenizer_mml:    # only chunk into blocklets on truncation altert
                    truncated = "Trctd!"
                    logging.info( f"%s - Build BS4 TEXT dict for chunker.#01..." % cmi_debug )
                    scentxt_d = dict()                                # init an empty dict
                    scentxt_d["0"] = scentxt[i].text
                    blocket_d = self.c4_chunker(scentxt_d, 100)   # TESTING DEBIGGING !!!! result = {} of blocklets
                    #blocket_d = self.c4_chunker(std, self.tokenizer_mml)   # result = {} of blocklets
                    for scentxt_k, scentxt_d in blocket_d.items():
                        logging.info( f"%s - Classification cycle: {scentxt_k}" % cmi_debug )
                        self.ttc, self.twc, cr = self.dict_processor(symbol, blocket_d)    # Exec AI NLP classifier inside dict_processor() !!
            return self.ttc, self.twc, cr
        
        '''
        else:   # BS4 -  NLP tokenization and count metrics - BS4 extracted dataset
            logging.info( f"%s - C4 engine.#1 - Transformer truncation preset: {self.tokenizer_mml}" % cmi_debug )
            chunked_raw_scentxt = self.c4_chunker(scentxt, self.tokenizer_mml)   # pre-process text into Blocklets < tokenizer_mml
            logging.info( f"%s - Text Blocklet rows generated: {len(chunked_raw_scentxt)} / total chars {(self.tokenizer_mml * len(chunked_raw_scentxt))}" % cmi_debug )
            # do NLP tokenization and count metrics - craw4ai extractor
            truncated = "Clean"
            logging.info( f"%s - Build BS4 TEXT dict for dict processor..." % cmi_debug )
            #std = dict()                        # clean dict
            #std["0"] = scentxt[i].text          # move line of text into a dict
            logging.info( f"%s - Exec NLP classfier.#01 @ BS4_eng.#01 - state: {truncated}..." % cmi_debug )
            self.ttc, self.twc, cr = self.dict_processor(symbol, scentxt)    # Exec AI NLP classifier inside dict_processor() !!
            return self.ttc, self.twc, cr

            self.dict_processor(symbol, chunked_raw_scentxt)      
        '''
    #####################################
    # Helper function
    def dict_processor(self, symbol, _text_dict):
        cmi_debug = __name__+"::"+self.dict_processor.__name__+".#"+str(self.yti)
        logging.info( f"%s - Dict text processor engine: {self.tokenizer_mml}" % cmi_debug )
        for i, chunk in _text_dict.items():    # cycle through all scentenses/paragraphs sent to us
            ngram_count = len(re.findall(r'\w+', chunk))
            ngram_tkzed = word_tokenize(chunk)
            self.ttc += int(len(ngram_tkzed))           # total vectroized tokensgenrated by tokenizer 
            if self.vectorz.is_scentence(chunk):
                chunk_type = "Scent"
            elif self.vectorz.is_paragraph(chunk):
                chunk_type = "Parag"
            else:
                chunk_type = "Randm"
            # INIT NLP classifier - WARN: truncating long scentences !!!
            logging.info( f"%s - Exec NLP classfier.#00 @ DICT_eng.#00..." % cmi_debug )
            clsfr_result = self.classifier(chunk, truncation=True)      # WARN: ???
            if self.args['bool_verbose'] is True:        # Logging level
                print ( f"Chunk: {i:03} / {chunk_type} / [ n-grams: {ngram_count:03} / tokens: {len(ngram_tkzed):03} / alphas: {len(chunk):03} ]", end="" )

            self.nlp_sent_engine(i, symbol, ngram_tkzed, ngram_count, clsfr_result[0])
        return ngram_tkzed, ngram_count, clsfr_result[0]

    #####################################
    # Helper function
    def c4_chunker(self, scentxt, tokenizer_mml):
        """
        Chunks a frame of text into smaller blocklets that do not exceed the LLM tokenizer max length
        to avoid truncation of text and ebale full text sentiment analysis (no loss of words)
        """        
        cmi_debug = __name__+"::"+self.c4_chunker.__name__+".#"+str(self.yti)

        if not scentxt:     # empty
            return {}       # for BS4, this is a row of <p> tag txt
        total_chars = sum(len(v) for v in scentxt.values())     # total of all chars in all rows
        logging.info( f"%s - Start article chunker @: {tokenizer_mml} rows: {len(scentxt)} / ({total_chars}) chars" % cmi_debug )
        logging.info( f"%s - Chunking Blockletts..." % cmi_debug )
        chunks = {}         # dict holds the final output. Key=0...n, value="blocklet of tesxt > tokenizer_mml"
        chunk_index = 0     # dict key
        start = 0           # text blocklet len counter
        while start < len(scentxt):
            end = start + tokenizer_mml     # Calculate end pos for this chunk
            if end >= len(scentxt):         # test for last chunk / exact boundary, take it as is
                chunk = scentxt[start:].strip()
                if chunk:                   # Only add non-empty chunks
                    logging.info( f"%s - Text Blocklet constructed: {chunk_index} @ {len(chunk)} chars" % cmi_debug )
                    chunks[chunk_index] = chunk
                break
     
            last_space = scentxt.rfind(' ', start, end) # Find last space within chunk to avoid breaking words
            if last_space == -1 or last_space <= start: # If no space, break at chunk_size
                chunk_end = end
            else:
                chunk_end = last_space
            chunk = scentxt[start:chunk_end].strip()    # Extract the chunk and add to list
            if chunk:   # Only add non-empty chunks
                logging.info( f"%s - Text Blocklet constructed: {chunk_index} @ {len(chunk)} chars" % cmi_debug )
                chunks[chunk_index] = chunk
                chunk_index += 1
                start = chunk_end + (1 if chunk_end < len(scentxt) and scentxt[chunk_end] == ' ' else 0)
        
        logging.info( f"%s - Chunker safely fabricated: {chunk_index+1} Text Blocklets" % cmi_debug )
        return chunks   # {} of perfect blockelts < tokenizer_mml
    
    # ##################################
    # Helper function
    def nlp_sent_engine(self, i, symbol, ngram_tkzed, ngram_count, clsfr_result):
        cmi_debug = __name__+"::"+self.nlp_sent_engine.__name__+".#"+str(self.yti)
        ngram_sw_remv = [word for word in ngram_tkzed if word.lower() not in self.stop_words]
        ngram_final = ' '.join(ngram_sw_remv)   # reform the scentence with stopwords removed
        hfw = []    # force hfw list to be empty
        try:
            if int(ngram_count) > 0:
                self.vectorz.reset_corpus(ngram_final)
                self.vectorz.fitandtransform()
                #vectorz.view_tdmatrix()     # Debug: dump Vectorized Tranformer info
                hfw = self.vectorz.get_hfword()
            else:
                hfw.append("Empty")
            self.twc += ngram_count    # save and count up Total Word Count
            ngram_sw_remv = ""
            ngram_final= ""
            ngram_count = 0     # words in scnentence/paragraph
            ngram_tkzed = 0     # vectorized tokens genertaed per scentence/paragraph
            sen_result = clsfr_result
            raw_score = sen_result['score']
            rounded_score = np.floor(raw_score * (10 ** 7) ) / (10 ** 7)
            
            if self.args['bool_verbose'] is True:        # Logging level
                print ( f" / HFN: {hfw} / Sent: {sen_result['label']} {(rounded_score * 100):.5f}%")

            logging.info( f'%s - Save chunklist to DF for article [ {self.item_idx} ]...' % cmi_debug )
            sen_package = dict(sym=symbol, urlhash=self.active_urlhash, article=self.item_idx, chunk=i, sent=sen_result['label'], rank=raw_score )
            self.save_sentiment(self.item_idx, sen_package)      # page, data
            self.sentiment_count[sen_result['label']] += 1  # count sentiment type
        except RuntimeError:
            print ( f"Model exception !!")
        except ValueError:
            print ( f"Empty vocabulary !!")
        except Exception as e:
            print ( f"ERROR sent engine !!: {e}")
    
        return
        #return self.ttc, self.twc, i

##################################### 3 ####################################
    def compute_precise_sentiment(self, symbol, df_final, positive_c, negative_c, positive_t, negative_t, neutral_t):
        """
        Compute precise sentiment analysis based on aggregated data from df_final
        
        Parameters:
        - symbol: Stock symbol for which sentiment is being computed
        - df_final: DF DataFrame containing aggregated all articles sentiment data (optional not used here)
        - positive_c: Total count of positive sentiment instances
        - negative_c: Total count of negative sentiment instances  
        - positive_t: Mean positive sentiment score
        - negative_t: Mean negative sentiment score
        - neutral_t: Mean neutral sentiment score
        - sentiment_categories: Dictionary mapping sentiment ranges to category descriptions
        
        Returns:
        - Dictionary containing precise sentiment metrics
        """
        cmi_debug = __name__+"::"+self.compute_precise_sentiment.__name__
        logging.info( f'%s - Computing precise sentiment analysis' % cmi_debug )

        # Step 1: Determine overall gross sentiment
        if positive_c > negative_c:
            gross_sentiment = "positive"
            posneg_ratio_pos = positive_c / negative_c if negative_c > 0 else positive_c
            posneg_ratio_neg = 0
            posneg_ratio = posneg_ratio_pos
        elif negative_c > positive_c:
            gross_sentiment = "negative" 
            posneg_ratio_pos = 0
            posneg_ratio_neg = negative_c / positive_c if positive_c > 0 else negative_c
            posneg_ratio = posneg_ratio_neg
        else:
            gross_sentiment = "neutral"
            posneg_ratio_pos = 1.0
            posneg_ratio_neg = 1.0
            
        # Step 2: Make the ratios bigger by factor of 100
        posneg_pos_big = posneg_ratio_pos * 100 if posneg_ratio_pos > 0 else 0
        posneg_neg_big = posneg_ratio_neg * 100 if posneg_ratio_neg > 0 else 0
        
        # Step 3: Compute percentage of information that is positive/negative
        total_sentiment = positive_c + negative_c
        if total_sentiment > 0:
            if gross_sentiment == "positive":
                data_pos_pct = (positive_c / total_sentiment) * 100
                data_neg_pct = (negative_c / total_sentiment) * 100
            else:
                data_pos_pct = (positive_c / total_sentiment) * 100  
                data_neg_pct = (negative_c / total_sentiment) * 100
        else:
            data_pos_pct = 0
            data_neg_pct = 0
            
        # Step 4: Compute precise sentiment scores
        if gross_sentiment == "positive":
            #precise_sent_pos = (posneg_pos_big - (positive_t * 100)) * neutral_t if neutral_t > 0 else posneg_pos_big - (positive_t * 100)
            #precise_sent_neg = (posneg_neg_big - (negative_t * 100)) * neutral_t if neutral_t > 0 else posneg_pos_big - (negative_t * 100)
            precise_sent_pos = (posneg_pos_big - (positive_t * 100)) * neutral_t
            precise_sent_neg = (posneg_neg_big - (negative_t * 100)) * neutral_t
        elif gross_sentiment == "negative":
            precise_sent_pos = ((positive_t * 100) - posneg_neg_big) * neutral_t if neutral_t > 0 else (positive_t * 100) - posneg_neg_big
            precise_sent_neg = ((negative_t * 100) - posneg_neg_big) * neutral_t if neutral_t > 0 else (negative_t * 100) - posneg_neg_big
        else:  # neutral
            precise_sent_pos = 0
            precise_sent_neg = 0
            
        # Round to integers
        precise_sent_pos = round(precise_sent_pos)
        precise_sent_neg = round(precise_sent_neg)
        
        # Step 5
        # HELPER function
        # - find the closest matching category for a given Category score       
        def find_closest_category(score, categories):
            """Find the closest matching category for a given score"""
            if not categories:
                return "Unknown"
            closest_key = min(categories.keys(), key=lambda x: abs(x - score))
            return categories[closest_key][0]  # Return the description string

        sentcat_pos = find_closest_category(precise_sent_pos, self.s_categories)
        sentcat_neg = find_closest_category(precise_sent_neg, self.s_categories)
        
        # Create results dictionary
        results = {
            'gross_sentiment': gross_sentiment,
            'data_pos_pct': data_pos_pct,
            'data_neg_pct': data_neg_pct,
            'precise_sent_pos': precise_sent_pos,
            'precise_sent_neg': precise_sent_neg,
            'sentcat_pos': sentcat_pos,
            'sentcat_neg': sentcat_neg,
            'posneg_ratio_pos': posneg_ratio_pos,
            'posneg_ratio_neg': posneg_ratio_neg,
            'posneg_intensity_ratio': posneg_ratio
        }
        
        if round(posneg_ratio,1) <= 1.5:
            gross_sentiment = "NEUTRAL"
        # Step 6: Print the precise sentiment metrics
        print( f"Overall:    {gross_sentiment.upper()} / Intensity: ({round(posneg_ratio,1)} : 1)" )
        print( f"Positivity: {data_pos_pct:.2f}% {sentcat_pos} @ Confidence: {(positive_t * 100):.2f}% / Cat score: {precise_sent_pos}" ) 
        print( f"Negativity: {data_neg_pct:.2f}% {sentcat_neg} @ Confidence: {(negative_t * 100):.2f}% / Cat score: {precise_sent_neg}" ) 

        sym = symbol
        pos_pct = f"{data_pos_pct:.2f}"
        neg_pct = f"{data_neg_pct:.2f}"

        self.s_data = [[ \
            sym, \
            gross_sentiment, \
            round(posneg_ratio,1), \
            pos_pct, \
            sentcat_pos, \
            precise_sent_pos, \
            neg_pct, \
            sentcat_neg, \
            precise_sent_neg, \
            positive_t, \
            negative_t, \
            neutral_t ]]
        
        self.df0_row = pd.DataFrame(self.s_data, columns=[ 'Symbol', 'Sentiment', 'Ratio', 'P_pct', 'P_cat', 'P_score', 'N_pct', 'N_cat', 'N_score', 'P_mean', 'N_mean', 'Z_mean' ] )
        self.sen_df3 = pd.concat([self.sen_df3, self.df0_row])
        logging.info( f'%s - Global Sentiment DF updated...' % cmi_debug )        

        return results

