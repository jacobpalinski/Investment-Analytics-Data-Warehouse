# Import modules
import pandas as pd
from transformers import pipeline, AutoTokenizer, TFAutoModelForSeq2SeqLM
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

def initialise_model() -> pipeline:
    '''
    Initialises finbert model
    '''
    model = pipeline("text-classification", model="ProsusAI/finbert", return_all_scores=True, truncation=True)
    return model

def initialise_tokenizer() -> AutoTokenizer:
    ''' 
    Initialises tokeniser for finbert model 
    '''
    tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
    return tokenizer

def calculate_sentiment_scores(batch_outputs) -> list:
    '''
    Function to generate final sentiment score for titles or descriptions
    '''
    scores = []
    for result in batch_outputs:
        sentiment_dict = {item['label'].lower(): item['score'] for item in result}
        score = (
            sentiment_dict.get('positive', 0) * 1.0 +
            sentiment_dict.get('neutral', 0) * 0.5 +
            sentiment_dict.get('negative', 0) * 0.0
        )
        scores.append(score)
    return scores

def process_text_with_chunking(model: pipeline, tokenizer: AutoTokenizer,  text: str, max_tokens: int = 512, chunk_size: int = 100) -> int:
    if not text or not isinstance(text, str) or text.strip() == "":
        return 0.5
    
    # Tokenize to check length
    tokens = tokenizer.tokenize(text)

    # If short enough, process directly
    if len(tokens) <= max_tokens:
        result = model([text], truncation=True, max_length=max_tokens)
        return calculate_sentiment_scores(result)[0]
    
    # If too long, split into chunks and average the scores
    chunks = []
    for i in range(0, len(tokens), chunk_size):
        chunk = tokens[i:i + chunk_size]
        chunk_text = tokenizer.convert_tokens_to_string(chunk)
        chunks.append(chunk_text)
    
    results = model(chunks, truncation=True, max_length=max_tokens)
    chunk_scores = calculate_sentiment_scores(results)
    return sum(chunk_scores) / len(chunk_scores)

def process_in_batches_short_description(model: pipeline, df: pd.DataFrame, text_column: str, batch_size: int = 500) -> list:
    ''' 
    Calculates sentiment scores for batches of rows within a dataframe where row text_column value contains < 512 tokens
    '''
    sentiment_scores = []
    for start in range(0, len(df), batch_size):
        end = start + batch_size
        texts = df[text_column].iloc[start:end].fillna("").astype(str).tolist()
        batch_outputs = model(texts, truncation=True, max_length=512)
        batch_scores = calculate_sentiment_scores(batch_outputs=batch_outputs)
        sentiment_scores.extend(batch_scores)
    return sentiment_scores

def process_in_batches_long_description(model: pipeline, tokenizer: AutoTokenizer, df: pd.DataFrame, text_column: str, batch_size: int = 500) -> list:
    ''' 
    Calculates sentiment scores for batches of rows within a dataframe where row text_column value can contain > 512 tokens
    '''
    sentiment_scores = []
    for start in range(0, len(df), batch_size):
        end = start + batch_size
        batch = df[text_column].iloc[start:end].fillna("").astype(str)
        for text in batch:
            score = process_text_with_chunking(model=model, tokenizer=tokenizer, text=text)
            sentiment_scores.append(score)
    return sentiment_scores