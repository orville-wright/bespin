# ml_sentiment.py — NLP Sentiment Analysis Engine

## Overview

`ml_sentiment.py` implements the `ml_sentiment` class — the core AI-powered sentiment analysis engine for the Bespin platform. It ingests raw article text extracted by either BeautifulSoup4 (BS4) or Crawl4ai (C4), tokenizes and chunks it to fit within the HuggingFace Transformer model's token limit, runs per-chunk inference via a fine-tuned financial-news sentiment model, and aggregates chunk-level scores into article-level and symbol-level sentiment metrics.

In addition to inference, the engine compresses raw article text with Zstandard (ZSTD) before writing to the LMDB key-value cache, and maintains a pandas DataFrame of per-chunk sentiment results for downstream aggregation.

---

## Position in the Bespin Pipeline

```
  Yahoo Finance / News Sites
          │
          ▼
  ml_yf_nlp_news_engine.py        ← Dual extractor (C4 + BS4), Depth 0–3
          │
          ▼
  ml_yf_nlp_orchestrator.py       ← Async coordinator
          │
          ▼
  ml_sentiment.py  ◄──────────────  THIS FILE
  (sentiment engine)
          │
          ├──► ml_cvbow.py         ← Bag-of-Words high-frequency word helper
          │
          ├──► LMDB KV cache       ← datastore_eng_LMDB.py (via cr_package)
          │
          └──► pandas DataFrame    ← sen_df0 (per-chunk), sen_df3 (summary)
```

`ml_sentiment` is instantiated from:
- `aop.py` (main orchestrator CLI)
- `xop.py` (streamlined CLI)
- `ml_yf_nlp_orchestrator.py` (async pipeline coordinator)
- `datastore_eng_LMDB.py` (cache rehydration path)

---

## Dependencies

| Library | Purpose |
|---------|---------|
| `transformers` (HuggingFace) | Sentiment classification pipeline |
| `pandas` | Per-chunk and summary DataFrames |
| `numpy` | Score rounding / math |
| `nltk` | Sentence tokenization, stopword removal |
| `sklearn` (via `ml_cvbow`) | Count Vectorizer (Bag-of-Words) |
| `zstandard` | ZSTD compression of article text blobs |
| `base64` | Base64 encoding of compressed blobs for JSON storage |
| `threading` | Background pre-loading of the HuggingFace model |
| `re` | n-gram word counting |
| `rich` | Terminal output formatting |

**HuggingFace model:**
```
mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis
```
This is a DistilRoBERTa model fine-tuned on financial news. It produces three-class output: `positive`, `negative`, `neutral` with a confidence score (0.0–1.0).

---

## Class: `ml_sentiment`

### Class-Level (Singleton) Attributes

These attributes are shared across all instances of the class and are used to implement a thread-safe singleton pattern for the HuggingFace classifier pipeline.

| Attribute | Type | Description |
|-----------|------|-------------|
| `_classifier` | `pipeline` or `None` | Singleton HuggingFace NLP pipeline (shared across all instances) |
| `_load_thread` | `Thread` or `None` | Background thread used during preload initialization |
| `_lock` | `threading.Lock` | Thread lock protecting `_classifier` from race conditions |

### Instance Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `active_urlhash` | `str` | SHA256 hash of the current article URL being processed |
| `args` | `dict` | Global configuration args passed in from `main()` |
| `art_buffer` | `list` | Buffer holding article text for processing |
| `blocket_udid` | `int` | Incrementing UID for inner blocklet sub-dicts |
| `chunk_udid` | `int` | Incrementing UID for outer chunk dict keys |
| `classifier` | `pipeline` | Instance reference to the shared HuggingFace pipeline |
| `cr_package` | `dict` | Full results dict produced by `dict_processor()` for one article |
| `cycle` | `int` | Thread loop counter |
| `_cs_count` | `int` | Count of sentence-type chunks in the current article |
| `_cp_count` | `int` | Count of paragraph-type chunks in the current article |
| `_cr_count` | `int` | Count of random/other-type chunks in the current article |
| `kv_json_dataset` | `dict` | Accumulated JSON dataset written to the LMDB KV cache |
| `df0_row_count` | `int` | Running count of rows added to `sen_df0` |
| `empty_vocab` | `int` | Counter for chunks where the BoW vectorizer found an empty vocabulary |
| `sen_df0` | `DataFrame` | Per-chunk sentiment results for the current article |
| `sen_cache_eng` | `int` | Count of chunks rehydrated from KV cache (vs. live inference) |
| `sen_llm_eng` | `int` | Count of chunks processed by the live LLM pipeline |
| `sen_df3` | `DataFrame` | Long-lived DataFrame collecting all sentiment summary data |
| `sen_data` | `list` | Staging list for a single DataFrame row before concat |
| `sentiment_count` | `dict` | `{'positive': N, 'negative': N, 'neutral': N}` for the current article |
| `tsenparas` | `int` | Total sentences and paragraphs seen |
| `ttc` | `int` | Total token count for the current article |
| `twc` | `int` | Total cumulative word count for the current article |
| `yti` | `int` | Instance UID (used in log prefixes) |
| `tokenizer_mml` | `int` | Model's max token length (set at runtime from the loaded pipeline) |

### Sentiment Category Lookup (`s_categories`)

A class-level dict mapping net sentiment score thresholds to human-readable labels. Each entry maps a score → `[label, score]`.

```python
s_categories = {
     1.0:  ['Extremley Bullish',  1.0],
     0.75: ['Strongly bullish',   0.75],
     0.50: ['Bullish',            0.50],
     0.125:['Positive',           0.125],
     0.00: ['Neutral',            0.00],
    -0.125:['Negative',          -0.125],
    -0.50: ['Bearish',           -0.50],
    -0.75: ['Strongly Bearish',  -0.75],
    -1.0:  ['Extremley Bearish', -1.0],
}
```

---

## Initialization

### `__init__(self, yti, global_args)`

Instantiates the class and initializes all per-article counters and data structures.

| Parameter | Type | Description |
|-----------|------|-------------|
| `yti` | `int` | Instance UID — used in all log line prefixes |
| `global_args` | `dict` | Global CLI/config args; must include `bool_verbose` key |

**Initializes:**
- `sentiment_count = {'positive': 0, 'negative': 0, 'neutral': 0}`
- `cr_package = {}`, `kv_json_dataset = {}`
- `chunk_udid = 0`, `blocket_udid = 0`
- `tsenparas = 0`, `empty_vocab = 0`
- `_cs_count`, `_cp_count`, `_cr_count` = 0

---

## Class Methods (Singleton Lifecycle)

### `preload_classifier(cls)` — `@classmethod`

Starts a background daemon thread to load the HuggingFace pipeline before it is needed. Call this once at program startup (before any articles are processed) to eliminate cold-start latency.

```python
ml_sentiment.preload_classifier()
```

- Safe to call multiple times — no-ops if preload is already in progress or complete.
- Sets `cls._load_thread` to track the background thread.
- Calls `_bg_load_worker()` in the background.

### `_bg_load_worker(cls)` — `@classmethod`

The background thread target. Imports `transformers.pipeline`, instantiates the model pipeline, and stores it in `cls._classifier` under the class lock.

- If model loading fails, logs an error — the main thread will fall back to cold-start loading.

### `_get_classifier(cls)` — `@classmethod`

Safely fetches the HuggingFace pipeline, handling two scenarios:

| Scenario | Behavior |
|----------|----------|
| Background thread started, not yet finished | Blocks on `join()` until load completes |
| Preload not called, or thread finished | Loads synchronously (cold start) |

Returns the ready `pipeline` instance. Called internally by `compute_sentiment()`.

---

## Instance Methods

### Method 1: `compute_sentiment(symbol, item_idx, scentxt, urlhash, ext)`

**The main entry point for sentiment analysis of a single article.**

Accepts raw article text, dispatches it to the correct pre-processing path (C4 or BS4), manages the chunk/blocklet pipeline, and returns aggregated results.

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `symbol` | `str` | Stock ticker symbol (e.g., `"AAPL"`) |
| `item_idx` | `int` | Article index number in the news candidate list |
| `scentxt` | `list` | Article text: a `list` of `str` (C4) or `list` of BS4 `<p>` tag elements (BS4) |
| `urlhash` | `str` | SHA256 hash of the article URL |
| `ext` | `int` | Extractor type: `0` = Crawl4ai, `1` = BS4 |

#### Returns

```python
(ttc: int, twc: int, cr_package: dict)
```

| Return Value | Description |
|-------------|-------------|
| `ttc` | Total token count for the article |
| `twc` | Total word count for the article |
| `cr_package` | Full results dict (see Output Data Format below) |

Returns `(0, 0, None)` if `scentxt` is empty.

#### Processing Flow

```
compute_sentiment()
    │
    ├── Initialize ml_cvbow vectorizer
    ├── Load HuggingFace classifier (via _get_classifier)
    ├── Compress article text via zstd_text_compressor()
    │
    ├── [Crawl4ai path (ext=0)]
    │     For each text blob in scentxt:
    │       ├── If len >= tokenizer_mml → unified_chunker() → dict_processor()
    │       └── If len < tokenizer_mml  → direct dict_processor()
    │
    └── [BS4 path (ext=1)]
          For each <p> element in scentxt:
            ├── If len > tokenizer_mml → unified_chunker() → dict_processor()
            └── If len <= tokenizer_mml → direct dict_processor()
```

#### Extractor Type Codes (`_dpro_eng`)

| Code | Extractor | State |
|------|-----------|-------|
| `1` | BS4 | Truncated (chunked) |
| `2` | BS4 | Clean (short, no chunking) |
| `3` | C4 | Truncated (chunked) |
| `4` | C4 | Clean (short, no chunking) |

---

### Method 2: `unified_chunker(st_list, tokenizer_mml, _ext_type, _curr_chunk_udid)`

Splits long text into "blocklets" — contiguous text chunks shorter than the model's max token length. Respects word boundaries (does not split words mid-token).

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `st_list` | `list[str]` | Input text as a list (typically one element) |
| `tokenizer_mml` | `int` | Model's max token length (from `tokenizer.model_max_length`) |
| `_ext_type` | `int` | Extractor type for logging (`0` = C4, `1` = BS4) |
| `_curr_chunk_udid` | `int` | Starting chunk UID (so UIDs are globally unique across calls) |

#### Returns

```python
(chunks: dict, chunk_index: int)
```

| Return Value | Description |
|-------------|-------------|
| `chunks` | `{uid: blocklet_text, ...}` — dict of numbered text blocklets |
| `chunk_index` | Next available UID (passed back into next call to maintain continuity) |

#### Algorithm Notes

- Uses `rfind(' ', start, end)` to find the last word boundary within the truncation window.
- Lists are used internally for O(1) indexed access over a dict for performance.
- Returns `{}` for empty input.

---

### Method 3: `dict_processor(symbol, _text_dict, _dpro_eng, _blocklet_udid)`

**The LLM inference core.** Iterates over a dict of text blocklets, runs the HuggingFace classifier on each, calls `nlp_sent_engine()` for further scoring, and assembles the per-article JSON result package.

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `symbol` | `str` | Stock ticker |
| `_text_dict` | `dict` | `{uid: text_string, ...}` blocklets from `unified_chunker` or directly from `compute_sentiment` |
| `_dpro_eng` | `int` | Extractor + truncation state code (1–4) |
| `_blocklet_udid` | `int` | Starting UID for sub-dict keys in the result package |

#### Returns

```python
(ttc: int, tnc: int, _x_cr_package: dict, element_udid: int)
```

| Return Value | Description |
|-------------|-------------|
| `ttc` | Total token count across all blocklets |
| `tnc` | Total n-gram (word) count across all blocklets |
| `_x_cr_package` | Accumulated JSON package for this dict of blocklets |
| `element_udid` | Next blocklet UID |

#### Per-Blocklet Processing

For each blocklet in `_text_dict`:

1. Count n-grams with `re.findall(r'\w+', chunk)`
2. NLTK-tokenize with `word_tokenize(chunk)`
3. Classify chunk type: sentence / paragraph / random (via `ml_cvbow`)
4. **Run HuggingFace classifier:** `self.classifier(chunk, truncation=True)`
5. Build a JSON sub-dict entry in `_x_cr_package`
6. Call `nlp_sent_engine()` for stopword removal, high-frequency word extraction, DF update

#### Output Sub-Dict Structure (per blocklet)

```python
_x_cr_package[f'{element_udid:03}'] = {
    'symbol':      str,   # ticker
    'chunk':       str,   # zero-padded chunk ID, e.g. '003'
    'n-grams':     str,   # word count, e.g. '087'
    'tokenz':      str,   # NLTK token count
    'alphas':      str,   # character count of this blocklet
    'sent_type':   str,   # 'positive' | 'negative' | 'neutral'
    'sent_score':  float, # 0.0–1.0 confidence from HuggingFace model
    'trct_state':  str,   # 'BS4_Trctd' | 'BS4_Clean' | 'C4_Trctd' | 'C4_Clean'
}
```

Top-level keys also added to `_x_cr_package`:

| Key | Description |
|-----|-------------|
| `urlhash` | Article URL SHA256 hash |
| `article` | Article index |
| `chunk_count` | Total chunks processed so far |
| `scentence` | Running sentence-type chunk count |
| `paragraph` | Running paragraph-type chunk count |
| `random` | Running random-type chunk count |

---

### Method 4: `nlp_sent_engine(_this_chunk, symbol, ngram_tkzed, ngram_count, _clsfr_result, _z_cr_package)`

Post-inference helper called per blocklet inside `dict_processor()`. Handles stopword removal, high-frequency word extraction via `ml_cvbow`, DF updating, and sentiment count tracking.

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `_this_chunk` | `str` | Zero-padded chunk ID string |
| `symbol` | `str` | Stock ticker |
| `ngram_tkzed` | `list` | NLTK tokens for this blocklet |
| `ngram_count` | `int` | Word count for this blocklet |
| `_clsfr_result` | `dict` | HuggingFace classifier output: `{'label': str, 'score': float}` |
| `_z_cr_package` | `dict` | Running results package (modified in-place) |

#### Returns

| Code | Meaning |
|------|---------|
| `0` | Success |
| `1` | `RuntimeError` (model inference failure) |
| `2` | `ValueError` (empty vocabulary — stopword-only text) |
| `3` | Other exception |

#### Processing Steps

1. Remove NLTK English stopwords from `ngram_tkzed`
2. Call `ml_cvbow.reset_corpus()` + `fitandtransform()` on the filtered text
3. Call `ml_cvbow.get_hfword()` to extract the highest-frequency word
4. Round the confidence score to 7 decimal places
5. Build `sen_package` dict
6. Call `save_sentiment_df()` to append a row to `sen_df0`
7. Increment `self.sentiment_count[label]`

---

### Method 5: `save_sentiment_df(item_idx, data_set, engine_id)`

Appends one row of sentiment data to `sen_df0`, the per-chunk in-memory DataFrame. Can be called from both the live LLM path and the KV cache rehydration path.

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `item_idx` | `int` | Article index |
| `data_set` | `dict` | `sen_package` dict: `{sym, urlhash, article, chunk, sent, rank}` |
| `engine_id` | `int` | `0` = KV cache rehydration, `1` = live LLM inference |

#### DataFrame Schema (`sen_df0`)

| Column | Description |
|--------|-------------|
| `Row` | Sequential row counter |
| `Symbol` | Ticker symbol |
| `art` | Article index |
| `urlhash` | URL hash |
| `chk` | Chunk ID |
| `rnk` | Raw confidence score |
| `snt` | Sentiment label: `positive` / `negative` / `neutral` |

---

### Method 6: `sentiment_metrics(symbol, df_final, positive_c, negative_c, positive_t, negative_t, neutral_t)`

**The final sentiment aggregation engine.** Combines article-count proportions with average per-chunk sentiment scores to produce a multi-dimensional sentiment summary. Implements a **2-vector sentiment model** standard in quantitative finance.

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `symbol` | `str` | Stock ticker |
| `df_final` | `DataFrame` | Final aggregated sentiment DataFrame |
| `positive_c` | `int` | Count of articles classified as positive |
| `negative_c` | `int` | Count of articles classified as negative |
| `positive_t` | `float` | Mean sentiment score across positive chunks |
| `negative_t` | `float` | Mean sentiment score across negative chunks |
| `neutral_t` | `float` | Mean sentiment score across neutral chunks |

#### Returns `None` if `total_articles == 0`, otherwise returns:

```python
{
    "symbol":            str,
    "net_sentiment":     float,  # [-1.0, +1.0] directional signal
    "confidence":        float,  # [0.0, 1.0]  dominant signal share
    "positive_share":    float,
    "neutral_share":     float,
    "negative_share":    float,
    "positive_strength": float,
    "neutral_strength":  float,
    "negative_strength": float,
    "positive_mean":     float,  # positive_t input
    "neutral_mean":      float,
    "negative_mean":     float,
    "positive_count":    int,
    "negative_count":    int,
}
```

#### The 2-Vector Model

```
Sentiment Vector:    (positive_strength - negative_strength) / total_strength
                     → directional signal [-1.0 to +1.0]

Uncertainty Vector:  neutral_strength / total_strength
                     → information / noise mass

Confidence:          max(positive_share, neutral_share, negative_share)
                     → dominant signal share
```

---

### Method 7: `sentiment_direction(symbol, net_sentiment, confidence, ...)`

Helper to `sentiment_metrics()`. Computes the human-readable sentiment band label for the net sentiment score, determines how far through the current band the score has progressed, and calls `sentiment_vector_model()`. Prints the full terminal sentiment summary report.

#### Sentiment Bands

| Range | Label |
|-------|-------|
| `[-1.00, -0.75)` | Extremely Bearish |
| `[-0.75, -0.50)` | Strongly Bearish |
| `[-0.50, -0.25)` | Bearish |
| `[-0.25,  0.00)` | Slightly Bearish |
| `[ 0.00,  0.25)` | Neutral |
| `[ 0.25,  0.50)` | Slightly Bullish |
| `[ 0.50,  0.75)` | Bullish |
| `[ 0.75,  1.00)` | Strongly Bullish |
| `[ 1.00]`        | Extremely Bullish |

When the score is ≥ 50% through the current band, the label becomes `"Approaching <next band>"`.

#### Terminal Report Format

```
Symbol:         AAPL
Sentiment:      Approaching Bullish   | Directionally biased -> Bullish
Base sentiment: Slightly Bullish
Band Progress:  73.4%  | through Slightly Bullish band
Signal clarity: 0.7823
Signal convctn: 0.4512 | Bullish
Net Score:      +0.342 | Sentiment Oscilator Direction
Signal purity:  68.2%  | Dominant Signal Share

Sentiment Composition:
Positivity:     55.3%  | (Directional signal mass:  0.432)
Neutrality:     21.8%  | (Non-directional ambiguity: 0.170)
Negativity:     22.9%  | (Directional signal mass:  0.179)
```

---

### Method 8: `sentiment_vector_model(positive_share, negative_share, neutral_share)`

Helper to `sentiment_direction()`. Implements the 2-vector signal model — separates directional signal from uncertainty noise to produce a final conviction score.

#### Parameters / Returns

```python
{
    "sentiment":       str,    # e.g. "Bullish", "Strongly Bearish"
    "direction_score": float,  # pos_dir - neg_dir in direction space (ignoring neutral)
    "clarity":         float,  # 1 - neutral_share (signal-to-noise)
    "conviction":      float,  # direction_score * clarity → final signal
    "pos_dir":         float,  # positive proportion of direction space
    "neg_dir":         float,  # negative proportion of direction space
}
```

#### Conviction Thresholds

| Conviction | Label |
|-----------|-------|
| `> 0.5`   | Strongly Bullish |
| `> 0.2`   | Bullish |
| `> 0.05`  | Slightly Bullish |
| `< -0.5`  | Strongly Bearish |
| `< -0.2`  | Bearish |
| `< -0.05` | Slightly Bearish |
| else      | Neutral |

---

### Method 9: `zstd_text_compressor(scentxt, _extractor)`

Compresses raw article text into a Base64-encoded ZSTD binary blob for storage in the LMDB cache.

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `scentxt` | `list` | Raw article text (same format as `compute_sentiment` input) |
| `_extractor` | `int` | `0` = Crawl4ai, `1` = BS4 |

#### Returns

`str` — Base64-encoded ZSTD-compressed article text blob (stored under key `'zstd_blob'` in `cr_package`)

#### Compression Details

- Uses ZSTD compression level `3` (balanced speed/ratio)
- Typical compression ratio: ~50% for English news text
- C4 path: encodes `scentxt[0]` directly (single text blob)
- BS4 path: joins all `<p>` element `.text` values with spaces before compressing

The `zstd_blob` is merged into `cr_package` at the end of `compute_sentiment()`, so it travels with all other sentiment results into the LMDB cache.

**Known limitation (v1.0):** The blob is stored as a Base64 JSON string, adding ~33% overhead. See the `TODO` comment in the source for the planned v2 architecture using raw msgpack binary storage.

---

## Output Data Format

### `cr_package` (returned by `compute_sentiment`)

The full results dict for one article. Structure:

```python
{
    # Top-level metadata
    'urlhash':     str,        # SHA256 URL hash
    'article':     int,        # article index
    'chunk_count': int,        # total chunks in this article
    'scentence':   int,        # sentence-type chunk count
    'paragraph':   int,        # paragraph-type chunk count
    'random':      int,        # random-type chunk count
    'zstd_blob':   str,        # Base64 ZSTD-compressed article text

    # Per-blocklet sub-dicts (zero-padded string keys)
    '000': {
        'symbol':     'AAPL',
        'chunk':      '000',
        'n-grams':    '087',
        'tokenz':     '097',
        'alphas':     '509',
        'sent_type':  'neutral',
        'sent_score': 0.9998877048492432,
        'trct_state': 'C4_Clean',
    },
    '001': { ... },
    '002': { ... },
    # ...
}
```

Real example output from `docs/NLP_sentiment_final_results.json`:

```python
{
    '000': {'alphas': '509', 'chunk': '000', 'n-grams': '087',
            'sent_score': 0.9998877048492432, 'sent_type': 'neutral',
            'symbol': 'FINV', 'tokenz': '097'},
    '001': {'alphas': '508', 'chunk': '001', 'n-grams': '084',
            'sent_score': 0.9973672032356262, 'sent_type': 'positive',
            'symbol': 'FINV', 'tokenz': '088'},
    '002': {'alphas': '510', 'chunk': '002', 'n-grams': '083',
            'sent_score': 0.9231919646263123, 'sent_type': 'positive',
            'symbol': 'FINV', 'tokenz': '089'},
    '003': {'alphas': '505', 'chunk': '003', 'n-grams': '091',
            'sent_score': 0.9927615523338318, 'sent_type': 'negative',
            'symbol': 'FINV', 'tokenz': '102'},
    'article': 1,
    'negative_count': 1,
    'neutral_count': 3,
    'positive_count': 2,
    'sent_paras': '006',
    'urlhash': '8c869a6df39e2d3179ba4b08ae5d1624699b1e031eae1ef4b90a108b3b94960a'
}
```

---

## Thread Safety

The HuggingFace pipeline is expensive to initialize (~2–5 seconds on first load). The class implements a thread-safe singleton pattern:

```
Application start
    │
    └── ml_sentiment.preload_classifier()   ← call once, non-blocking
            │
            └── Spawns daemon thread → _bg_load_worker()
                    │
                    └── Imports transformers, loads model, stores in cls._classifier

Article processing (any time after)
    │
    └── instance.compute_sentiment(...)
            │
            └── _get_classifier()
                    │
                    ├── If thread still running: join() (blocks briefly)
                    └── If thread done or not started: returns cls._classifier directly
```

All access to `cls._classifier` is guarded by `cls._lock` (a `threading.Lock()`).

---

## Usage Example

```python
from ml_sentiment import ml_sentiment

# Pre-warm the model in the background at program startup
ml_sentiment.preload_classifier()

# ... later, when you have article text ...

args = {'bool_verbose': True}
sent_ai = ml_sentiment(yti=1, global_args=args)

# scentxt: list of text blobs (C4) or list of BS4 <p> elements (BS4)
# ext=0 for Crawl4ai, ext=1 for BS4
total_tokens, total_words, results = sent_ai.compute_sentiment(
    symbol='AAPL',
    item_idx=0,
    scentxt=['Full article text blob here ...'],
    urlhash='abc123...',
    ext=0
)

# Run final aggregation once all articles are processed
summary = sent_ai.sentiment_metrics(
    symbol='AAPL',
    df_final=some_df,
    positive_c=3,
    negative_c=1,
    positive_t=0.92,
    negative_t=0.88,
    neutral_t=0.97
)
print(summary['net_sentiment'])   # e.g. 0.3421
print(summary['confidence'])      # e.g. 0.6812
```

---

## Flow Diagram

See [`diagrams/ml_sentiment_flow.mermaid`](diagrams/ml_sentiment_flow.mermaid) for a visual representation of the full processing pipeline.

---

## Version History

| Version | Date | Notes |
|---------|------|-------|
| 1.0.0 | 2026-06-18 | Initial README — comprehensive documentation of ml_sentiment.py generated by automated codebase analysis |
