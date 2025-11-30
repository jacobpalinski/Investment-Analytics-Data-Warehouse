# Import modules
import sys
from unittest.mock import MagicMock

# Block TensorFlow
mock_tf = MagicMock()
sys.modules["tensorflow"] = mock_tf
sys.modules["tensorflow.python"] = mock_tf
sys.modules["tensorflow.keras"] = mock_tf
sys.modules["keras"] = mock_tf
sys.modules["jax"] = MagicMock()
sys.modules["jaxlib"] = MagicMock()

# Mock heavy transformers module before it can be imported
mock_transformers = MagicMock()
mock_transformers.pipeline = MagicMock()
mock_transformers.AutoTokenizer = MagicMock()
mock_transformers.TFAutoModelForSeq2SeqLM = MagicMock()

sys.modules["transformers"] = mock_transformers
