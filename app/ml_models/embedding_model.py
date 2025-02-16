"""
All methods necessary for the NLP part of the application:
- get_embeddings(arr)
- ...
"""
import os
from sentence_transformers import SentenceTransformer


def load_model(save_dir):
    """
    Load a sentence-transformer model from the specified path.

    Parameters:
    - save_dir (str): Path to model.

    Returns:
    - model: sentenceTransformer model.
    """
    assert os.path.isdir(save_dir)
    model = SentenceTransformer(save_dir)
    return model


model_path = './model'
#model_path = '/code/model'

model = load_model(model_path)


def get_embeddings(arr):
    '''
    Calculating the embeddings for the given Strings using a sentencetransformer model.

    Parameters:
    - arr (List<String>): A List/Array of Strings to be encoded.

    Returns:
    - Embeddings (List): Either a list of embeddings or a List of a single embedding.
    '''
    output = model.encode(arr)
    return output.tolist()