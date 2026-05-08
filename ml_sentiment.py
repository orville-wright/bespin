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
import zstandard as zstd

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
    blocket_udid = 0    # working blocklet UID
    chunk_udid = 0      # working chunk UID
    classifier = None   # NLP classidier pipeline - real AI model LLM computation. GPU goes brrrr....!!
    _classifier = None  # optomized singleton (Class attribute) NLP classidier pipeline (class global)
    cr_package = None   # full reslts dict{} of dict_processor run
    cycle = 0           # class thread loop counter
    _cs_count = 0       # scentence count
    _cp_count = 0       # paragraph count
    _cr_count = 0       # random text block count
    kv_json_dataset = None  # JSON dataset to be used for kvstore
    df0_row_count = 0
    empty_vocab = 0     # tracker that LLM found empty vocab
    mlnlp_uh = None     # URL Hinter instance
    sen_df0 = None      # sentiment for this artile ONLY (gets overwritten each time per article)
    sen_df1 = None      # uNUSED
    sen_df2 = None      # ? unknown
    sen_df3 = None      # A long lasting DF to collect all sentiment data
    sen_data = []       # Data to be added to the DataFrame
    sentiment_count = None  # Sentiment counts for this article
    tsenparas = 0       # total sentences & paragraphs
    ttc = 0             # Total Tokens generated in the scnetcne being analyzed
    twc = 0             # Total cumulative Word count in this artcile being analyzed
    yti = 0
    
    # Techcnial analysys dict defines sentiment score to description mapping
    s_categories = {
            225: (['Bullishly positive', 225]),
            100: (['Trending bullish', 100]),
            50: (['Positive', 50]),
            25: (['Trending positive', 25]),
            0: (['Neutral', 0]),
            -25: (['Trending negative', -25]),
            -50: (['Negative', -50]),
            -100: (['Somewhat Bearish', -100]),
            -225: (['Bearishly negative', -225])
            }
        
    ######################## init ##########################################
    def __init__(self, yti, global_args):
        cmi_debug = __name__+"::"+self.__init__.__name__
        logging.info( f'%s   Instantiate.#{yti}' % cmi_debug )
        self.args = global_args                            # Only set once per INIT. all methods are set globally
        self.yti = yti
        self.cr_package = dict()
        self.tsenparas = 0
        self.empty_vocab = 0
        self.kv_json_dataset = dict()  # inti the JSON dataset
        self.chunk_udid = int(0)
        self.blocket_udid = int(0)
        self.sentiment_count = { 'positive': 0, 'negative': 0, 'neutral': 0 }
        self._cs_count = 0
        self._cp_count = 0
        self._cr_count = 0
        
        # Initialzie the HF NLP classifier pipeline ONCE on class init.
        # this is the real AI model LLM computation. GPU goes brrrr....!!
        if ml_sentiment._classifier is None:
            logging.info( f'%s - Init HF classifier model pipeline: mrm8488/distilroberta...' % cmi_debug )
            ml_sentiment._classifier = pipeline(
                task="sentiment-analysis",
                model="mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis"
            )
        # Reference the shared model
        self.classifier = ml_sentiment._classifier      # initialize the class classifier
        self.tokenizer_mml = self.classifier.tokenizer.model_max_length  # initalize the class tokenizer
        return

    # #################################### 1
    def compute_sentiment(self, symbol, item_idx, scentxt, urlhash, ext):
        """
        called by:  BS4_artdata_depth3 -> compute_sentiment(symbol, item_idx, local_stub_news_p, hs, 1)
                    C4_artdata_depth3  -> compute_sentiment(symbol, item_idx, local_stub_news_p, hs, 0)
        INPUTS:
        1. symbol = ticker symbol
        2. item_idx = the index num in the ml_index DB (i.e. the list of candidate news articles)
        3. local_stub_news_p = list[] of text tags or C4 TEXT blob extracted from the article
        4. urlhash = hash of the url
        5. ext = extractor type (0 = Crawl4ai, 1 = BS4)
        
        Tokenize and compute scentence chunk sentiment
        scentxt = BS4 all <p> zones that look/feel like scentence/paragraph text
                = Crawl4ai its 1 bulk block of article text
        WARN:  scentext is the article test, but it may need to eb pre-0processed depending on which extractor was used (i.e. BS4 or C4)
        WARN: BS4 extracts html <p> tag elements, NOT the simple raw text. <p> tages contain the text but must be treated as a raw html page data.
              crawl4ai extracts the bulk raw text and discards all surrounding html tags, dumping a single blob of text into a list[].
              crawl4ai text must be chunked @ model truncation length, i.e.  tokenizer_mml (e.e.g 512 chars)
              BS4 also checks LLM truncation length but <p> tags contain short text strings, and are less likely to be truncated.... but theres more of them.  
        """
        #if self.args['bool_verbose'] is True:        # Logging level
        self.yti = item_idx
        self.active_urlhash = urlhash
        cmi_debug = __name__+"::"+self.compute_sentiment.__name__+".#"+str(self.yti)
        self.item_idx = item_idx
        self.ext_type = ext
        logging.info( f'%s     - Init NLP Tokenizor, Vectorizer & Stopwords engine.#{self.ext_type}...' % cmi_debug )
        
        self.vectorz = ml_cvbow(item_idx, self.args)    # scikit-lean NLP M/L
        self.stop_words = stopwords.words('english')
        _dpro_eng = 0

        # Initialzie the HF NLP LLM classifier pipeline
        # this is the real AI model LLM computation. GPU goes brrrr....!!
        #logging.info( f'%s - Init HF classifier model pipeline: mrm8488/distilroberta...' % cmi_debug )
        #self.classifier = pipeline(task="sentiment-analysis", model="mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis")
        #self.tokenizer_mml = self.classifier.tokenizer.model_max_length

        self.ttc = 0
        self.twc = 0
        self.cr = None
        self.final_results = dict()     
        self.sentiment_count["positive"] = 0
        self.sentiment_count["negative"] = 0
        self.sentiment_count["neutral"] = 0

        _ext_decoder = {
            0: "C4ai",
            1: "BS4"
        }
        
        if len(scentxt) == 0:
            self.final_results.update({ 'noart_data': 1 })
            logging.info( f"%s - ERROR [{_ext_decoder.get(ext, 'Extractor?')}] Article has 0-len text from Depth 3 (multiple reasons!)" % cmi_debug )
            return 0, 0, None    # this is prob an error waiting to happen (prob needs chunker dict @ var 3)
        else:
            pass
        
        #################################################
        # Main control logic
        # Compute sentiment for 1 article Full TEXT block
        #
        
        # Crawl4ai pre-processor
        #
        if self.ext_type == 0:      # Craw4ai
            logging.info( f"%s - C4 Blocklet Builder engine.#1 - LLM Trnctn {self.tokenizer_mml} / rows: {len(scentxt)} input: {type(scentxt)}" % cmi_debug )
            # input MUST be a crawl4ai prepred list of full article text. 
            # c4 strips out all html and<dumps <p> tag text elements into 1 big text blob in a list[ ]
            # therfore chunker has  to do a more work for c4
            _i_twc = 0              # reset counters (class global attributes)
            self.ttc = 0            # " "
            self.twc = 0            # " "
            self._cs_count = 0      # " "
            self._cp_count = 0      # " "
            self._cr_count = 0      # " "
            self.chunk_udid = 0     # reset main chunk subdict key udid - chunkid
            self.blocket_udid = 0   # reset internal subdict key udid
            self.cr_package = dict()            # reset the cr_package for each <p> tag processed
            self._chunk_profile = dict()        # what type of chunk thisd is (sent/para/randm)
            self.kv_json_dataset = dict()       # reset the GLOBAL JSON dict. hold full blocklet JSON struct for this article
            self._chunk_profile = { 'scentence': 0, 'paragraph': 0, 'random': 0 }

            _zstd_article_blob = self.zstd_text_compressor(scentxt, self.ext_type)       # compress article text into a ZSTD binary blob
            
            for i in range(0, len(scentxt)):    # this = 1 b/c C4 sends a list[] of 1 big blob of text
                logging.info( f"%s - C4 Eval pre-chunker @row: {i:03} / TEXT length: {len(scentxt[i])} chars" % cmi_debug )
                truncated = "Undef"
                if len(scentxt[i]) >= self.tokenizer_mml:   # self.tokenizer_mml: only chunk into blocklets on truncation alert
                    truncated = "Truncation!"
                    _dpro_eng = 3                  # C4 + Truncated
                    logging.info( f"%s - {truncated} Long text blocklet / sending to unfied_chunker.#1..." % cmi_debug )
                    blocklet_l = list()
                    blocklet_l.append(scentxt[i])  # stack full article text -> list[]. uified_chunker takes list[] input only
                    blocklet_d, self.chunk_udid = self.unified_chunker(blocklet_l, self.tokenizer_mml, self.ext_type, self.chunk_udid)   # send list[], result = {} of blocklets
                    self.ttc, _i_twc, _tr_final_results, self.blocket_udid = self.dict_processor(symbol, blocklet_d, _dpro_eng, self.blocket_udid)    # Exec AI NLP classifier inside dict_processor() !!
                    self.twc += _i_twc
                    self.cr_package.update(_tr_final_results)  # merge the final results into the cr_package
                    continue
                else:
                    truncated = "Clean"
                    _dpro_eng = 4   # C4 + Clean (not truncated)
                    logging.info( f"%s - {truncated} Short text blocklet / No truncation" % cmi_debug )
                    blocklet_d = dict()
                    blocklet_d.update({self.chunk_udid: scentxt[i]})    # create 1 row dict for dict_processor() for NATURAL short/clean text blocklet
                    self.chunk_udid += 1
                    self.ttc, _i_twc, _cl_final_results, self.blocket_udid = self.dict_processor(symbol, blocklet_d, _dpro_eng, self.blocket_udid)    # send dict{}, Exec AI NLP classifier inside dict_processor() !!
                    self.twc += _i_twc
                    self.cr_package.update(_cl_final_results)  # merge the final results into the cr_package
                    continue

            self.cr_package.update({'zstd_blob': _zstd_article_blob})  # merge the ZSTD compressed binary blob into the _x_cr_package dict
            self.blocket_udid = 0   # after this entire article is processed, reset the blocklet counter
            return self.ttc, self.twc, self.cr_package
        
        # BS4 pre-processor
        #
        else:
            logging.info( f"%s - BS4 Blocklet Builder engine.#1 - LLM Trnctn @ {self.tokenizer_mml} / rows: {len(scentxt)} in: {type(scentxt)}" % cmi_debug )
            # WARN: must be a BS4 prepared list of article text
            # - BS4 only sends a list of rows of individual <p> tags element
            # - 1 at a time from within the article body
            # The chunker has good probablity of not doing as much work as C4 b/c BS4 <p> text fragemtns are shorter
            self.ext_type = 1   # BS4
            _i_twc = 0              # reset counters (class global attributes)
            self.ttc = 0            # " "
            self.twc = 0            # " "
            self._cs_count = 0      # " "
            self._cp_count = 0      # " "
            self._cr_count = 0      # " "
            self.chunk_udid = 0     # reset main chunk subdict key udid
            self.blocket_udid = 0   # reset internal subdict key udid
            self.cr_package = dict()            # reset the cr_package for each <p> tag processed
            self._chunk_profile = dict()        # what type of chunk thisd is (sent/para/randm)
            self.kv_json_dataset = dict()       # reset the GLOBAL JSON dict. hold full blocklet JSON struct for this article
            self._chunk_profile = { 'scentence': 0, 'paragraph': 0, 'random': 0 }

            _zstd_article_blob = self.zstd_text_compressor(scentxt, self.ext_type)    # compress article text into a ZSTD binary blob

            for i in range(0, len(scentxt)):    # this = num of rows of <p> tag text
                logging.info( f"%s - BS4 Eval pre-chunker @row: {i:03} / TEXT length: {len(scentxt[i].text)} chars" % cmi_debug )   # cycle through all scentenses/paragraphs sent to us
                truncated = "Undef"
                if len(scentxt[i].text) > self.tokenizer_mml:      # only chunk into blocklets on truncation altert
                    truncated = "Truncation!"
                    _dpro_eng = 1   # BS4 + Truncated
                    logging.info( f"%s - {truncated} Long text blocklet / send LIST to unfied_chunker.#1..." % cmi_debug )
                    blocklet_l = list()
                    blocklet_l.append(scentxt[i].text) # create 1 row list[], extracting <p> text (from html.element) for dict_processor() ( needs chunking)
                    blocklet_d, self.chunk_udid = self.unified_chunker(blocklet_l, self.tokenizer_mml, self.ext_type, self.chunk_udid)   # send = list[], result = {} of chunked blocklets
                    self.ttc, _i_twc, _tr_final_results, self.blocket_udid = self.dict_processor(symbol, blocklet_d, _dpro_eng, self.blocket_udid)    # Exec AI NLP classifier inside dict_processor() !!
                    self.twc += _i_twc 
                    self.cr_package.update(_tr_final_results)  # merge the final results into the cr_package
                    continue
                else:
                    truncated = "Clean"
                    _dpro_eng = 2   # BS4 + Clean (not truncated)
                    logging.info( f"%s - {truncated} Short text blocklet / No truncation" % cmi_debug )
                    blocklet_d = dict()
                    blocklet_d.update({self.chunk_udid: scentxt[i].text}) # create 1 row dict for dict_processor() (ths is a short/clean <p>) text blocklet
                    self.chunk_udid += 1
                    self.ttc, _i_twc, _cl_final_results, self.blocket_udid = self.dict_processor(symbol, blocklet_d, _dpro_eng, self.blocket_udid)    # send dict{}, Exec AI NLP classifier inside dict_processor() !!
                    self.twc += _i_twc
                    self.cr_package.update(_cl_final_results)  # merge the final results into the cr_package
                    continue
    
            self.cr_package.update({'zstd_blob': _zstd_article_blob})  # merge the ZSTD compressed binary blob into the _x_cr_package dict
            self.blocket_udid = 0   # after this entire article is processed, reset the blocklet counter
        return self.ttc, self.twc, self.cr_package
    
    # #################################### 2
    # Helper function
    def unified_chunker(self, st_list, tokenizer_mml, _ext_type, _curr_chunk_udid):
        """
        Unified chunker
        Chunks a frame of article text data into smaller blocklets to fit within LLM tokenizer max length
        which is fixed when the LLM is built and trained.

        INFO:
        - input **must** be a list[ ]
        - lists provide O(1) indexed access, which are 2-3x more memory efficent than a dict{}
        - lists optomize for index/slice lookups, dicts{} optomize for key lookups

        Avoids truncation of text and enbales full text sentiment analysis (no loss of words)
        Honnors word boundaries on chunking logic (doesnt split a word)
        Leverages list[] slicing, b/c dicts dont provide slices

        WARN:
        input -> list [ ] and resulting ouput .> dict{ }
        - For a C4 input list of text (i.e. a single element list of and a very long bloob of text)...

        RESULT:
        - a dict{} of beautifully chunked "blocklets" shorter than tokenizer_mml
        - could easily be a multi element {} if input is a long text string
        """        
        cmi_debug = __name__+"::"+self.unified_chunker.__name__+".#"+str(self.yti)
        logging.info( f"%s   - Unified chunking engine @ truncation: {self.tokenizer_mml}" % cmi_debug )

        ext_type_decode = {
            0: "C4_extr",
            1: "BS4_extr"
        }

        if not st_list:     # empty
            return {}       # for BS4, this is a row of <p> tag txt

        abs_tchars = sum(len(s) for s in st_list)   # total of all chars in all rows
        logging.info( f"%s   - Start {ext_type_decode.get(_ext_type, 'Unknown')} chunker - chars: {abs_tchars} @ trctn: {tokenizer_mml}" % cmi_debug )
        chunks = {}         # dict holds the final output. Key=0...n, value="blocklet of text > tokenizer_mml"
        self.chunk_index = _curr_chunk_udid     # sub-dict key
        end = start = 0     # text blocklet positional indexers
        run_total = 0       # cumulative total

        while start < abs_tchars:               # continue if there more text to come
            end = start + tokenizer_mml         # setup a tuncation window
            if end >= abs_tchars:               # test if end would overrun end of text string
                blocklet = st_list[0][start:abs_tchars]      # Extract the chunk text and add to a holding list:]
                if blocklet:                    # only work on non-empty chunks
                    _b = len(blocklet)          # get len of this chunk (allways at live loc list[0])
                    end = start + _b            # compute end index point of this short-tail blocklet (should = abs_tchars)
                    #print (f"###debug-314: Blocklet tail overrun - {start:04} / len: {_b} / end: {end} / max: {abs_tchars}")
                    #print (f"{blocklet}")
                    chunks[self.chunk_index] = blocklet      # add to final trail output to dict DATA PACKAGE
                    run_total += _b
                    _remaining = abs_tchars - run_total
                    logging.info( f"%s - Eng.#1 Blocklet: {self.chunk_index:03} Contains:  {len(blocklet):03} chars @ index [ {start:04} -> {abs_tchars:04} ] / remaining [ {_remaining:04} ] chars" % cmi_debug )
                    self.chunk_index += 1
                break       # end the entire while loop - should be the END of all chars

            st_string = f"{st_list[0]}"                     # convert list[0] to string for rfind()
            last_space = st_string.rfind(' ', start, end)   # Find last space in chunk avoid breaking words
            #print (f"##-@251: lspace:{last_space} / len:{len(st_list[0])}")
            if last_space == -1 or last_space <= start:     # If no space (-1), break at chunk_size
                chunk_end = end
                #print (f"##-@254: at the end!")
            else:
                chunk_end = last_space
                blocklet = st_list[0][start:chunk_end]      # Extract the chunk and add to a holding list
                #print (f"##-@258: blocklet:{blocklet} / end:{chunk_end} / last:{last_space}")
                
            if blocklet:                                    # only add non-empty chunks
                chunks[self.chunk_index] = blocklet         # add to final output dict DATA PACKAGE
                #print ( f"2_udid:{self.chunk_index:03} ", end="")  # debug
                _b = len(blocklet)
                run_total += _b
                #run_total += int(len(blocklet[0]))
                #print (f"##-@267: runtot:{run_total} / chunk:{self.chunk_index}")
                #print (f"##-@267: runtot:{run_total} / chunk:{self.chunk_index}")
                _remaining = abs_tchars - run_total
                logging.info( f"%s - Eng.#2 Blocklet: {self.chunk_index:03} Contains:  {len(blocklet):03} chars @ index [ {start:04} -> {run_total:04} ] / remaining [ {_remaining:04} ] chars" % cmi_debug )
                start = chunk_end + (1 if chunk_end < len(st_list) and st_list[chunk_end] == ' ' else 0)
                self.chunk_index += 1

        return chunks, self.chunk_index   # {} of perfect blockelts < tokenizer_mml
    
    # #################################### 3
    # LLM Helper function
    def dict_processor(self, symbol, _text_dict, _dpro_eng, _blocklet_udid):
        '''
        This engine processes a dict{} of text blocklets (scentences/paragraphs)
        - for 1 article ONLY
        - 1 set of blocklets could be either truncated or clean
        - truncated due to being longer than the LLMN truncation limit
        -  clean... shorter than the LLM truncation limit
        - It executes the LLM NLP Classifier pipeline on each blocklet within the full input dict
        
        WARN: can only intake a dict{} of text blocklets
        - The UNIFEID_CHUNKER prepares chunks into a nice dict{} of blocklets
        - Heavy CPU / GPU utilization will be triggered NOW !
        '''
        cmi_debug = __name__+"::"+self.dict_processor.__name__+".#"+str(self.yti)
        logging.info( f"%s - Initialize EMPTY DICT processor engine..." % cmi_debug )

        tc = 0
        ttc = 0
        twc= 0
        tnc = 0
        ngram_count = 0
        self.element_udid = _blocklet_udid
        # truncation status micro lookup dict
        dpro_eng_decode = {
            0: "Unknonwn",
            1: "BS4_Trctd",
            2: "BS4_Clean",
            3: "C4_Trctd",
            4: "C4_Clean"
        }

        _x_cr_package = dict()      # ensure cr_packge is local and empty dict !
        cmi_debug = __name__+"::"+self.dict_processor.__name__+".#"+str(self.yti)
        logging.info( f"%s - Start DICT processor engine:  {dpro_eng_decode.get(_dpro_eng, 'Unknown')}" % cmi_debug )
        
        # main control loop
        for _chunk_udid, chunk in _text_dict.items():     # cycle through all scentenses/paragraphs sent to us
            ngram_count = len(re.findall(r'\w+', chunk))  # count of words (ngrams)
            ngram_tkzed = word_tokenize(chunk)            # split TEXT chunk into NLP LLM tokens !! output -> list[]
            twc += ngram_count                            # cumulative total word count
            tc += int(len(ngram_tkzed))                   # cumulative vectroized tokens genrated by tokenizer 
            # whole doccument metrics
            if self.vectorz.is_scentence(chunk):
                self._cs_count += 1
                self._chunk_profile.update(scentence=self._cs_count)
                self._chunk_type = "scent"
            elif self.vectorz.is_paragraph(chunk):
                self._cp_count += 1
                self._chunk_profile.update(paragraph=self._cp_count)
                self._chunk_type = "parag"
            else:
                self._cr_count += 1
                self._chunk_profile.update(random=self._cr_count)
                self._chunk_type = "randm"

            _truncate_state = dpro_eng_decode.get(_dpro_eng, 'Unknown')  # decode the _dpro_eng var

            logging.info( f"%s - ======== LLM Classifying Blocklet: {_chunk_udid:03} via: {_truncate_state} ========" % cmi_debug)
            logging.info( f"%s - ======== Exec LLM Sentiment classifier/vectorizor  ==================" % cmi_debug )
            
            ####################### LLM NLP #######################
            # THIS IS THE HEAVY LIFTING - LLM CLASSIFIER PIPELINE #
            #######################################################
            #
            clsfr_result = self.classifier(chunk, truncation=True)      # LLM sentimewnt classifier NOW !!!
            #
            #print (f"DP-chunk: {_chunk_udid:03} ({clsfr_result[0]['score']}) ", end="" )
            #print ( f"##-@320: CHUNK: {_chunk_udid:03}  {dpro_eng_decode.get(_dpro_eng, 'Unknown')}\n{chunk}" )
            
            # build a JSON package for this chunk
            # - add base JSON elements -> chunk metrics
            _x_cr_package.update(self._chunk_profile)      # add final whole doc metrics to package
            #
            if _x_cr_package.get('urlhash') is None:
                _x_cr_package['urlhash'] = self.active_urlhash
            if _x_cr_package.get('article') is None:
                _x_cr_package['article'] = self.item_idx
            if _x_cr_package.get("chunk_count") is None:                # count is a ZEROTH ordinal number (starts from 0)
                _x_cr_package.update({'chunk_count': (_chunk_udid)})    # add to package - current GLOBAL class var
            else:
                _x_cr_package['chunk_count'] = self.chunk_udid          # count is a ZEROTH ordinal number (starts from 0)
            
            # TODO:
            # merge the ZSTD compressed binary blob (i.e. full article text)
            # into the _x_cr_package dict as part of the primary dict (not in any of the sub-dicts)
            #
            # ---> HERE <---


            ##################################################################
            # Built a dataset as a dict to write into LMBD KV cache
            # formated JSON dict{} package
            # subdict KEY is element_udid  (e.g. 001)
            # 
            _k = f'{self.element_udid:03}' 
            _x_cr_package[_k]=({            # KEY to create this subdict for this chunk
                            'symbol': symbol,
                            'chunk': f"{_chunk_udid:03}",
                            'n-grams': f"{twc:03}",
                            'tokenz': f"{len(ngram_tkzed):03}",
                            'alphas': f"{len(chunk):03}",
                            'sent_type': clsfr_result[0]['label'],
                            'sent_score': clsfr_result[0]['score'],
                            'trct_state': _truncate_state
                            })

            ttc += tc   # total tokenz count
            tnc += twc  # total word count
            if self.args['bool_verbose'] is True:        # Logging level
                print ( f"Chunk: {_chunk_udid:03} / Type: {self._chunk_type} / Words: {tnc:03} / tokenz: {len(ngram_tkzed):03} / alphas: {len(chunk):03} ", end="" )

            _this_chunk = f'{_chunk_udid:03}'     # format chunk
            _ec = self.nlp_sent_engine(_this_chunk, symbol, ngram_tkzed, ngram_count, clsfr_result[0], _x_cr_package)
            match _ec:
                case 0:
                    # merge this single row dict dataset with JSON dataset for this article
                    # json dataset keeps growing as each chunk row is processed...
                    self.kv_json_dataset.update(_x_cr_package)  # merge/extend KV cache JSON dataset
                    self.element_udid += 1    # ensure we're adding a new subdict to the JSON dataset
                    continue
                case 1:
                    print (f"LLM exception")
                    continue
                case 2:
                    print ( f"chunk: {self.empty_vocab} / empty vocab @:{_chunk_udid:03} ", end="" )
                    self.empty_vocab += 1
                    continue
                case 3:
                    print ("Vectorizor error!")
                    continue
                case _:
                    print ("Unknown LLM/Vect error!")
        return ttc, tnc, _x_cr_package, self.element_udid

    # #################################### 4
    # LLM Helper function for dict_processor()
    def nlp_sent_engine(self, _this_chunk, symbol, ngram_tkzed, ngram_count, _clsfr_result, _z_cr_package):
        """
        - Computes sentimnent SCORES !!!
        RETURN codoes: 0, 1, 2, 3
            0 = Success
            1 = RuntimeError
            2 = Empty Vocab
            3 = Other Exception
        - removes stopwords (generic non-domain word noise that dilute sentiment scoring)
        - Calculates High Frequency Words inside the HOT classified LLM Transformer
        - prepares Global DF update results package
        - calls save_sentiment_df() to update sentiment metrics
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
            ngram_count = 0                 # words in scnentence/paragraph
            ngram_tkzed = 0                 # vectorized tokens genertaed per scentence/paragraph
            sen_result = _clsfr_result      # positive/negative/neutral from LLM classifier
            raw_score = sen_result['score'] # score from LLM classifier
            rounded_score = np.floor(raw_score * (10 ** 7) ) / (10 ** 7)
            
            if self.args['bool_verbose'] is True:        # Logging level
                print ( f" / HFN: {hfw} / Sent: {sen_result['label']} {(rounded_score * 100):.5f}%")

            logging.info( f'%s - Save blocklet metrics to DF for article [ {self.item_idx} ]...' % cmi_debug )
            sen_package = dict(sym=symbol,
                               urlhash=self.active_urlhash,
                               article=self.item_idx,
                               chunk=_this_chunk,
                               sent=sen_result['label'],
                               rank=raw_score )
            
            self.save_sentiment_df(self.item_idx, sen_package)      # page, data
            self.sentiment_count[sen_result['label']] += 1  # count sentiment type
            # INFO: sentiment_count{ 'positive': 0, 'negative': 0, 'neutral': 0 }
            # WARN: sentiment_count doesnt get computed during KV Deep Cahce REHYRATE mode b/c the LLM is not executed
            return 0
        except RuntimeError:
            print ( f"Model exception !!")
            return 1
        except ValueError:
            #print ( f"Empty vocabulary: {self.empty_vocab} / ", end="" )
            #self.empty_vocab += 1
            return 2
        except Exception as e:
            print ( f"ERROR sent engine !!: {e}")
            return 3

    # #################################### 5
    def save_sentiment_df(self, item_idx, data_set):
        """
        Save key ML sentiment info to global sentimennt in-memory Dataframe
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
        logging.info( f"%s - Rehydrate metrics DF @ article: {item_idx} / chunk: {chk:03} / {snt} / score: {rnk}" % cmi_debug )
        return

    # #################################### 6
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


    # #################################### 7
    def zstd_text_compressor(self, scentxt, _extractor):
        """
        Compresses article text into a ZSTD binary blob
        So we can store it in the article LMDB KV cache entry
        - ZSTD is a fast compression algorithm, low CPU utilizaiton, good compression ratios ~50% for text data
        - C4 sends a list of 1 big blob of htlp striped text (all <p> tags text combined into 1 big blob)
        - BS4 sends a list of rows of individual <p> tags html element text (needs careful prep-prossing)
        
        WARNING:
        This is an initial template implementation...
        - my v1 LMDB storage archietcure is inefficent and contains hidden bloat. It needs to be redesigned as per below.
        
        TODO:
        1. Dont use Base64 JSON encoding. It adds a 33% overhead in size.
        2. Use msgpack() raw binary packing of ZSTD compressed text binary blob into LMDB
        3. Force LMBD into raw binary storage mode for the ZSTD blob data (raw=True)
        4. Use "Minified" dict key strategy in the _cr_package dict. Avoid "Key Name Tax", reduce size of JSON package
        5. Convert urlhash into 32 raw bytes <bytes.fromhex()> instead of 64-char hex string. Lean efficent storage +  faster lookups
        
        - migrate _cr_package dict and LMDB to the above.
          It is Lean, more efficient, faster encoding, fast lookups, more scalable.
        """
        cmi_debug = __name__+"::" + self.zstd_text_compressor.__name__+".#"+str(_extractor)
        logging.info( f"%s - article text compressor..." % cmi_debug )
        if _extractor == 0:      # C4
            logging.info( f"%s - C4 ZSTD text compressor engine..." % cmi_debug )
            # C4 sends a list of 1 big blob of text (all <p> tags text combined into 1 big blob)
            # print ( f"ARTICLE_STARTS_HERE: {scentxt[0]}")     # for debugging...
            _source_data = scentxt[0].encode('utf-8')   # prepare byte stream for ZSTD compressor
            _compressor = zstd.ZstdCompressor(level=3)
            compressed_C4_blob = _compressor.compress(_source_data)
            _perctg_compressed = len(compressed_C4_blob) / len(_source_data) * 100
            logging.info( f"%s - Orig size: {len(_source_data)} bytes / Cmprssd size: {len(compressed_C4_blob)} bytes / optz: {_perctg_compressed:.2f} pct" % cmi_debug )
            return compressed_C4_blob
        elif _extractor == 1:    # BS4
            logging.info( f"%s - BS4 ZSTD text compressor engine..." % cmi_debug )
            _blocklets = ["ARTICLE_STARTS_HERE:"]
            _blocklets.extend([item.text for item in scentxt])  # low mem usage, fast list comprehension
            #
            # INFO:
            # Active pattern is the FASTEST execution path via intermediate list extend comprehension 
            # Good memory usage... $O(n) (linear)
            # instead of $O(n^2) (quadratic) for basic imutable string concatination memory trap
            #  
            # But, this pattern (below) is BEST memory utilizaiton, but not as fast a list comprehension
            # generator memory optomized pattern, but slower than list comprehension
            #_final_article = "ARTICLE_STARTS_HERE: " + " ".join(item.text for item in scentxt)
            #
            _final_article = " ".join(_blocklets)
            # print ( f"{_final_article}")      # for debugging...
            _source_data = _final_article.encode('utf-8')   # prepare byte stream for ZSTD compressor
            _compressor = zstd.ZstdCompressor(level=3)
            compressed_BS4_blob = _compressor.compress(_source_data)
            _perctg_compressed = len(compressed_BS4_blob) / len(_source_data) * 100
            logging.info( f"%s - Orig size: {len(_source_data)} bytes / Cmprssd size: {len(compressed_BS4_blob)} bytes / optz: {_perctg_compressed:.2f} pct" % cmi_debug )
            return compressed_BS4_blob

        return 1
