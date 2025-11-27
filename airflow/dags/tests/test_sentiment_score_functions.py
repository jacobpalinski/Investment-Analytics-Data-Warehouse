# Import modules
import pytest
import pandas as pd
from dags.fact_sentiments.sentiment_score_functions import (
    calculate_sentiment_scores,
    process_text_with_chunking,
    process_in_batches_short_description,
    process_in_batches_long_description
)

class TestSentimentScoreFunctions:

    def test_calculate_sentiment_scores(self):
        batch_outputs = [
            [
                {"label": "positive", "score": 0.7},
                {"label": "neutral", "score": 0.2},
                {"label": "negative", "score": 0.1},
            ]
        ]

        result = calculate_sentiment_scores(batch_outputs)
        assert pytest.approx(result, 0.0001) == [0.8]


    def test_process_text_with_chunking_empty_text(self, mocker):
        model = mocker.Mock()
        tokenizer = mocker.Mock()

        assert process_text_with_chunking(model, tokenizer, "") == 0.5
        assert process_text_with_chunking(model, tokenizer, "   ") == 0.5
        assert process_text_with_chunking(model, tokenizer, None) == 0.5


    def test_process_text_with_chunking_short_text(self, mocker):
        model = mocker.Mock()
        tokenizer = mocker.Mock()

        tokenizer.tokenize.return_value = ["hello", "world"]
        tokenizer.convert_tokens_to_string.side_effect = lambda x: " ".join(x)

        # Pipeline output mock (must match pipeline call signature)
        model.side_effect = lambda texts, **kwargs: [
            [
                {"label": "positive", "score": 0.7},
                {"label": "neutral", "score": 0.2},
                {"label": "negative", "score": 0.1},
            ]
        ]

        result = process_text_with_chunking(model, tokenizer, "hello world")
        assert pytest.approx(result, 0.0001) == 0.8


    def test_process_text_with_chunking_long_text(self, mocker):
        model = mocker.Mock()
        tokenizer = mocker.Mock()

        tokenizer.tokenize.return_value = ["text"] * 250
        tokenizer.convert_tokens_to_string.side_effect = lambda toks: " ".join(toks)

        # Each chunk returns its own sentiment distribution
        mock_outputs = [
            [
                {"label": "positive", "score": 0.5},
                {"label": "neutral", "score": 0.4},
                {"label": "negative", "score": 0.1},
            ],
            [
                {"label": "positive", "score": 0.6},
                {"label": "neutral", "score": 0.3},
                {"label": "negative", "score": 0.1},
            ],
            [
                {"label": "positive", "score": 0.8},
                {"label": "neutral", "score": 0.1},
                {"label": "negative", "score": 0.1},
            ],
        ]

        # Pipeline returns a list of lists (one per chunk)
        model.side_effect = lambda texts, **kwargs: [mock_outputs[i] for i in range(len(texts))]

        result = process_text_with_chunking(model, tokenizer, "x" * 1000, max_tokens=50, chunk_size=100)

        expected = (0.7 + 0.75 + 0.85) / 3
        assert pytest.approx(expected, 0.0001) == result

    def test_process_in_batches_short_description(self, mocker):
        model = mocker.Mock()
        df = pd.DataFrame({"text": ["text1", "text2", "text3"]})

        # Pipeline returns output for ALL items in batch
        mock_outputs = [
            [
                {"label": "positive", "score": 0.5},
                {"label": "neutral", "score": 0.4},
                {"label": "negative", "score": 0.1},
            ],
            [
                {"label": "positive", "score": 0.6},
                {"label": "neutral", "score": 0.3},
                {"label": "negative", "score": 0.1},
            ],
            [
                {"label": "positive", "score": 0.8},
                {"label": "neutral", "score": 0.1},
                {"label": "negative", "score": 0.1},
            ],
        ]

        outputs_iter = iter(mock_outputs)
        model.side_effect = lambda texts, **kwargs: [next(outputs_iter) for _ in texts]

        result = process_in_batches_short_description(model, df, "text", batch_size=2)

        assert result == pytest.approx([0.7, 0.75, 0.85])


    def test_process_in_batches_long_description(self, mocker):
        model = mocker.Mock()
        tokenizer = mocker.Mock()

        df = pd.DataFrame({"text": ["text1", "text2", "text3"]})

        # Tokenizer returns a short list → no chunking
        tokenizer.tokenize.return_value = ["text"]

        # model should return LIST OF LISTS (batch → predictions)
        # because calculate_sentiment_scores expects: [ [ {label, score}, ... ] ]
        model.side_effect = [
            [[{"label": "positive", "score": 0.5},
            {"label": "neutral", "score": 0.4},
            {"label": "negative", "score": 0.1}]],

            [[{"label": "positive", "score": 0.6},
            {"label": "neutral", "score": 0.3},
            {"label": "negative", "score": 0.1}]],

            [[{"label": "positive", "score": 0.8},
            {"label": "neutral", "score": 0.1},
            {"label": "negative", "score": 0.1}]]
        ]

        result = process_in_batches_long_description(
            model, tokenizer, df, "text", batch_size=1
        )

        expected = [0.7, 0.75, 0.85]
        assert result == pytest.approx(expected)
