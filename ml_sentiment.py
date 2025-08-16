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
    args = []           # class dict to hold global args being passed in from main() methods
    art_buffer = []     # Buffer to hold article text for processing
    classifier = None   # NLP classidier pipeline
    cr_package = None   # full reslts dict{} of dict_processor ruin
    cycle = 0           # class thread loop counter
    df0_row_count = 0
    empty_vocab = 0     # tracker that LLM found empty vocab
    mlnlp_uh = None     # URL Hinter instance
    sen_df0 = None      # sentiment for this artile ONLY (gets overwritten each time per article)
    sen_df1 = None      # uNUSED
    sen_df2 = None      # ? unknown
    sen_df3 = None      # A long lasting DF to collect all sentiment data
    sen_data = []       # Data to be added to the DataFrame
    sentiment_count = { 'positive': 0, 'negative': 0, 'neutral': 0 }  # Sentiment counts for this article
    tsenparas = 0       # total sentences & paragraphs
    ttc = 0             # Total Tokens generated in the scnetcne being analyzed
    twc = 0             # Total cumulative Word count in this artcile being analyzed
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
        self.cr_package = dict()
        self.tsenparas = 0
        self.empty_vocab = 0
        return

##################################### 2 ####################################
    def compute_sentiment(self, symbol, item_idx, scentxt, urlhash, ext):
        """
        called by:  extract_article_data -> compute_sentiment(symbol, item_idx, local_stub_news_p, hs, 0)
        INPUTS:
        1. symbol = ticker symbol
        2. item_ida = the index num in the ml_index DB
        3. scentxt = list[] of multiple <p> tags from articel containg individual article text strings
        4. urlhash = hash of the url
        5. ext = extractor type (0 = Crawl4ai, 1 = BS4)
        
        Tokenize and compute scentence chunk sentiment
        scentxt = BS4 all <p> zones that look/feel like scentence/paragraph text
                = Crawl4ai its 1 bulk block of article text
        WARN: scentxt is a list of BS4 extracted html <p> htlm elements, NOT the raw text. It must be treated as a html data row.
              crawl4ai extarcts the bulk raw text in 1 list[] and discards the HTML <p> tags.
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
        self.cr = None
        self.final_results = dict()
        self.final_results["sent_paras"] = 0
        self.sentiment_count["positive"] = 0
        self.sentiment_count["negative"] = 0
        self.sentiment_count["neutral"] = 0
        self.active_urlhash = urlhash
        if len(scentxt) == 0:
            self.final_results.update({ 'noart_data': 1 })
            logging.info( f"%s - ERROR Crawl4ai no article skimming data from Depth 0 (multiple reasons possible)!" % cmi_debug )
            return 0, 0, 0    # this is prob an error waiting to happen (prob needs chunker dict @ var 3)
        else:
            pass
        if self.ext_type == 0:
            logging.info( f"%s - Crawl4ai engine.#1 LLM Truncation @ {self.tokenizer_mml} / rows: {len(scentxt)} input: {type(scentxt)}" % cmi_debug )
            # input MUST be a crawl4ai prepred list of full article text. 
            # c4 dumps all <p> tage text elements into 1 big list - this is how crawl4ai works !!
            # therfore chunker has a higher probabliy of needing to do a lot more work for c4
            for i in range(0, len(scentxt)):
                logging.info( f"%s - Eval bulk TEXT length: {len(scentxt[i])} chars" % cmi_debug )
                truncated = "Undef"
                if len(scentxt[i]) >= self.tokenizer_mml: # self.tokenizer_mml:    # only chunk into blocklets on truncation altert
                    truncated = "Trctd!"
                    blocklet_l = list()
                    blocklet_l.append(scentxt[i])  # force create a full article text list, since chunker needs this input structure
                    blocklet_d = self.unified_chunker(blocklet_l, self.tokenizer_mml, self.ext_type)   # send = list[], result = {} of blocklets
                    logging.info( f"###-debug-130 - Status: {truncated} - blocklet_d: {type(blocklet_d)}" )
                    self.ttc, _c_twc, final_results = self.dict_processor(symbol, blocklet_d)    # Exec AI NLP classifier inside dict_processor() !!
                    self.twc += _c_twc
                else:
                    truncated = "Clean"
                    logging.info( f"%s - No truncation: {truncated} Short text blocklet" % cmi_debug )
                    blocklet_d = dict()
                    blocklet_d.update({i: scentxt[i]})    # create 1 row dict for dict_processor() (ths is a NATURAL short/clean text blocklet
                    self.ttc, _c_twc, final_results = self.dict_processor(symbol, blocklet_d)    # send = dict{}, Exec AI NLP classifier inside dict_processor() !!
                    self.twc += _c_twc

                #print (f"##-debug-141: {truncated}:\n{blocklet_d}\n\n {final_results}")
            return self.ttc, self.twc, final_results
        else:
            logging.info( f"%s - BS4 engine.#1 - LLM Truncation @ {self.tokenizer_mml} / rows: {len(scentxt)} in: {type(scentxt)}" % cmi_debug )
            # WARN: must be a BS4 prepared list of article text
            # - BS4 only sends a list of each individual <p> tags element
            # - 1 at a time from within the articel body
            # The chunker has good probablity of not  doing as much work as C4 b/c BS4 <p> text fragemtns are shorter
            bs4rows = int(len(scentxt))
            bs4_row = 0
            for i in range(0, len(scentxt)):
                logging.info( f"%s - Eval TEXT char length: {len(scentxt[i].text)}" % cmi_debug )   # cycle through all scentenses/paragraphs sent to us
                truncated = "Undef"
                if len(scentxt[i].text) > self.tokenizer_mml:      # only chunk into blocklets on truncation altert
                    truncated = "Trctd!"
                    logging.info( f"%s - Send BS4 TEXT LIST to chunker.#01..." % cmi_debug )
                    blocklet_l = list()
                    #blocklet_l.update({ bs4_row: scentxt[i].text }) # create 1 row dict for dict_processor() (Too Long <p>) text blocklet needs chunking)
                    blocklet_l.append(scentxt[i].text) # create 1 row list[], extracting <p> text (from html.element) for dict_processor() ( needs chunking)
                    blocklet_d = self.unified_chunker(blocklet_l, self.tokenizer_mml, self.ext_type)   # send = list[], result = {} of chunked blocklets
                    self.ttc, _i_twc, final_results = self.dict_processor(symbol, blocklet_d)    # Exec AI NLP classifier inside dict_processor() !!
                    self.twc += _i_twc
                    bs4_row += 1
                else:
                    truncated = "Clean"
                    logging.info( f"%s - No truncation: {truncated} Short text blocklet" % cmi_debug )
                    blocklet_d = dict()
                    blocklet_d.update({bs4_row: scentxt[i].text}) # create 1 row dict for dict_processor() (ths is a short/clean <p>) text blocklet
                    self.ttc, _i_twc, final_results = self.dict_processor(symbol, blocklet_d)    # send dict{}, Exec AI NLP classifier inside dict_processor() !!
                    self.twc += _i_twc
                    bs4_row += 1
                    self.cr_package.update({ 'sent_paras': int(self.tsenparas) })
                    
            return self.ttc, self.twc, final_results
    
    #####################################
    # Helper function
    def unified_chunker(self, st_list, tokenizer_mml, ext_type):
        """
        Unifided chunker
        Chunks a frame of article text data into smaller blocklets not exceeding LLM tokenizer max length
        WARN: input **must** be a list[]
        - lists provide O(1) indexed access. Are 2-3x more memory efficent than a dict{}
        - lists optomize for index/slice lookups, dicts{} optomize for key lookups 
        Avoids truncation of text and ebale full text sentiment analysis (no loss of words)
        Honnors word boundaries on chunking logic
        Leverages list[] slicing, b/c dicts dont provide slices
        Result is an dict{} of beautifullt chunked "blocklets"
        Result could potentially be a muti element {} if input data is a long text string       
        """        
        cmi_debug = __name__+"::"+self.unified_chunker.__name__+".#"+str(self.yti)

        if not st_list:     # empty
            return {}       # for BS4, this is a row of <p> tag txt
        #total_chars = sum(len(v) for v in st_list.values())     # total of all chars in all rows
        abs_tchars = sum(len(s) for s in st_list)            
        logging.info( f"%s - Start chunker for chars: {abs_tchars} @ truncation: {tokenizer_mml}" % cmi_debug )
        chunks = {}         # dict holds the final output. Key=0...n, value="blocklet of text > tokenizer_mml"
        chunk_index = 0     # dict key
        start = 0           # text blocklet len counter
        run_total = 0       # cumulative total
        #print (f"###-DEBUG-192: in-1:{type(st_list)} / in-2:{tokenizer_mml} / in-3{ext_type}")
        while start < abs_tchars:
            end = start + tokenizer_mml     # Calculate end pos for this chunk
            #print (f"###-DEBUG-194: start:{start} / end:{end} / tkml:{tokenizer_mml} / abschars:{abs_tchars}")
            if end >= abs_tchars:           # test if end would overrun max len of chunk
                chunk = st_list[start:][:end]
                if chunk:                   # do we have a non-empy chunk? only add non-empty chunks
                    run_total += len(chunk[0])
                    #print (f"###-DEBUG-199: run:{run_total} / len:{len(chunk[0])}")
                    logging.info( f"%s - Eng.#1 Blocklet constructed: {chunk_index:03} @ {len(chunk[0]):03} chars [ {run_total:04} ]" % cmi_debug )
                    chunks[chunk_index] = chunk     # add to final output dict
                    #print (f"###-DEBUG-202: chkidx:{chunk_index} / chunk:\n{chunk}")
                break
 
            st_string = f"{st_list[0]}"
            last_space = st_string.rfind(' ', start, end) # Find last space within chunk to avoid breaking words
            #print (f"###-DEBUG-207: lspace:{last_space} / len:{len(st_list[0])}")
            if last_space == -1 or last_space <= start: # If no space (-1), break at chunk_size
                chunk_end = end
                #print (f"###-DEBUG-211: at the end!")
            else:
                chunk_end = last_space
                blocklet = st_list[0][start:chunk_end]        # Extract the chunk and add to list
                #print (f"###-DEBUG-215: blocklet:{blocklet} / end:{chunk_end} / last:{last_space}")
                
            if blocklet:   # Only add non-empty chunks
                chunks[chunk_index] = blocklet         # add to final output dict
                chunk_index += 1
                _b = len(blocklet)
                run_total += _b
                #run_total += int(len(blocklet[0]))
                #print (f"###-DEBUG-223: runtot:{run_total} / chunk:{chunk_index}")
                logging.info( f"%s - Eng.#2 Blocklet constructed: {chunk_index:03} @ {len(blocklet):03} chars [ {run_total:04} ]" % cmi_debug )
                start = chunk_end + (1 if chunk_end < len(st_list) and st_list[chunk_end] == ' ' else 0)

        return chunks   # {} of perfect blockelts < tokenizer_mml
    
    #####################################
    # Helper function
    def dict_processor(self, symbol, _text_dict):
        '''
        WARN: LLM Classifier can only intake a dict{}
        This function EXECUTES the LLM NLP Classified pipeline
        - Heavy CPU utilization will be triggered
        '''
        tc = 0
        ttc = 0
        twc= 0
        ngram_count = 0
        tnc = 0
        _x_cr_package = dict()      # ensure cr_packge is loca and empty !
        cmi_debug = __name__+"::"+self.dict_processor.__name__+".#"+str(self.yti)
        logging.info( f"%s - Global chunking engine @ truncation: {self.tokenizer_mml}" % cmi_debug )
        for i, chunk in _text_dict.items():    # cycle through all scentenses/paragraphs sent to us
            ngram_count = len(re.findall(r'\w+', chunk))  # count of words (ngrams)
            ngram_tkzed = word_tokenize(chunk)            # split TEXT chunk into NLP LLM tokens !! output -> list[]
            twc += ngram_count                            # cumulative total word count
            tc += int(len(ngram_tkzed))                   # cumulative vectroized tokens genrated by tokenizer 
            if self.vectorz.is_scentence(chunk):
                chunk_type = "Scent"
                self.tsenparas += 1                       # keep count of Total scentences
            elif self.vectorz.is_paragraph(chunk):
                chunk_type = "Parag"                      # keep count of Total paragraphs
                self.tsenparas += 1
            else:
                chunk_type = "Randm"
            logging.info( f"============ LLM Classifying Blocklet ============================================")
            logging.info( f"%s - Exec NLP classfier.#00 @ DICT_eng.#00..." % cmi_debug )
            clsfr_result = self.classifier(chunk, truncation=True)      # input = chunk {} - 1 element
            _k = f'{i:03}'  # nicly formated dict{} key
            _x_cr_package[_k] = ({
                            'symbol': symbol,
                            'chunk': f"{i:03}",
                            'n-grams': f"{twc:03}",
                            'tokenz': f"{len(ngram_tkzed):03}",
                            'alphas': f"{len(chunk):03}",
                            'sent_type': clsfr_result[0]['label'],
                            'sent_score': clsfr_result[0]['score'] })

            # add element outside of chunk element 
            _x_cr_package.update({ 'sent_paras': int(self.tsenparas) })
            ttc += tc
            tnc += twc
            if self.args['bool_verbose'] is True:        # Logging level
                print ( f"Chunk: {i:03} / {chunk_type} / [ Words: {tnc:03} / tokenz: {len(ngram_tkzed):03} / alphas: {len(chunk):03} ]", end="" )
                
            _tc = f'{i:03}'     # format chunk
            final_results = self.nlp_sent_engine(_tc, symbol, ngram_tkzed, ngram_count, clsfr_result[0], _x_cr_package)
        return ttc, tnc, final_results

 ###################
    # Helper function for dict_processor()
    def nlp_sent_engine(self, i, symbol, ngram_tkzed, ngram_count, clsfr_result, _z_cr_package):
        """
        - removes stopwords
        - Calculates High Frequency Words inside the HOT classified LLM Transformer
        - Computes sentimnent scores
        - prepares Global DF update results package
        - calls save_sentiment_df() to updaet sentiment metrics
        - tracks global sentiment count metrics for (Pos, Neg, Neutral)
        - completes results_package
        - reports LLM Transform model classifiation excpetions
        """
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
            self.save_sentiment_df(self.item_idx, sen_package)      # page, data
            self.sentiment_count[sen_result['label']] += 1  # count sentiment type
            _z_cr_package.update({
                            'urlhash': self.active_urlhash,
                            'article': self.item_idx,
                            })
        except RuntimeError:
            print ( f"Model exception !!")
        except ValueError:
            print ( f"Empty vocabulary: {self.empty_vocab} / ", end="" )
            self.empty_vocab += 1
        except Exception as e:
            print ( f"ERROR sent engine !!: {e}")
    
        return _z_cr_package      # dict{}
        #return self.ttc, self.twc, i

##################################### 1 ####################################
    def save_sentiment_df(self, item_idx, data_set):
        """
        Save key ML sentiment info to global sentimennt Dataframe
        data_set = a dict
        """
        self.yti
        cmi_debug = __name__+"::"+self.save_sentiment_df.__name__+".#"+str(self.yti)
        x = self.df0_row_count      # get last row added to DF
        x += 1

        # need to add the url hash in here, otherwise I cant do useful analysis
        sym = data_set["sym"]
        art = data_set["article"]
        urlhash = data_set["urlhash"]
        chk = data_set["chunk"]
        rnk = data_set["rank"]
        snt = data_set["sent"]

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
        logging.info( f"%s - Sent DF Updated / Art: {item_idx} / chunk: {chk:03} / sent: {snt} / score: {rnk}" % cmi_debug )
        return

##################################### 3 ####################################
    def sentiment_metrics(self, symbol, df_final, positive_c, negative_c, positive_t, negative_t, neutral_t):
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
        cmi_debug = __name__+"::"+self.sentiment_metrics.__name__
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
            posneg_ratio = 1.0
            
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

        self.s_data = [[
            sym,
            gross_sentiment,
            round(posneg_ratio,1),
            pos_pct,
            sentcat_pos,
            precise_sent_pos,
            neg_pct,
            sentcat_neg,
            precise_sent_neg,
            positive_t,
            negative_t,
            neutral_t ]]
        
        self.df0_row = pd.DataFrame(self.s_data, columns=[ 'Symbol', 'Sentiment', 'Ratio', 'P_pct', 'P_cat', 'P_score', 'N_pct', 'N_cat', 'N_score', 'P_mean', 'N_mean', 'Z_mean' ] )
        self.sen_df3 = pd.concat([self.sen_df3, self.df0_row])
        logging.info( f'%s - Global Sentiment DF updated...' % cmi_debug )        

        return results
