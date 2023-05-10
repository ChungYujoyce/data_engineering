"""
create ModelHandler and use spaCy for inference.
"""
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.ml.inference.base import RunInference, ModelHandler, PredictionResult

import spacy
from spacy import Language
from typing import Any, Dict, Iterable, Optional, Sequence

import warnings
warnings.filterwarnings("ignore")

text_strings = [
    "The New York Times is an American daily newspaper based in New York City with a worldwide readership.",
    "It was founded in 1851 by Henry Jarvis Raymond and George Jones, and was initially published by Raymond, Jones & Company."
]

# Start building the pipeline
pipeline = beam.Pipeline()

class SpacyModelHandler(ModelHandler[str,
                                     PredictionResult,
                                     Language]):
    def __init__(self, model_name: str = 'en_core_web_sm'):
        self.model_name = model_name

    def load_model(self) -> Language:
        return spacy.load(self.model_name)
    
    def run_inference(self, batch: Sequence[str], model: Language,
                      inference_args: Optional[Dict[str, Any]] = None
                        ) -> Iterable[PredictionResult]:
        preds = []
        for one_text in batch:
            doc = model(one_text)
            preds.append([(ent.text, ent.start_char, ent.end_char, ent.label_) for ent in doc.ents])

        return [PredictionResult(x, y) for x, y in zip(batch, preds)]
        

# Print the results for verification.
with pipeline as p:
    (p 
    | "CreateSentences" >> beam.Create(text_strings)
    | "RunInferenceSpacy" >> RunInference(SpacyModelHandler("en_core_web_sm"))
    | beam.Map(print)
    )