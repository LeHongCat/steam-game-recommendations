"""Run a pinned multilingual E5 encoder on CPU and score catalog descriptions."""

import argparse
import hashlib
import logging
import re
import uuid
from functools import lru_cache
from statistics import median
from urllib.request import urlopen

from src import storage
from src.recommendation_query import TAGS

MODEL_ID = "intfloat/multilingual-e5-small"
REPO = "Xenova/multilingual-e5-small"
REVISION = "761b726dd34fb83930e26aab4e9ac3899aa1fa78"
FILES = {
    "tokenizer.json": ("tokenizer.json", "0b44a9d7b51c3c62626640cda0e2c2f70fdacdc25bbbd68038369d14ebdf4c39"),
    "model.onnx": (
        "onnx/model_quantized.onnx",
        "f80102d3f2a1229f387d3c81909990d8945513e347b0eab049f7de3c6f98c193",
    ),
}
MODEL_DIR = storage.ROOT / "data/recommendation/query-model"
SEMANTIC_ONLY_WEIGHT = 0.85
MIN_BEST_SIMILARITY = 0.79
MIN_SIMILARITY_SPREAD = 0.035
NEAR_MATCH_WINDOW = 0.035
logger = logging.getLogger(__name__)


def install():
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    for name, (source, expected) in FILES.items():
        target = MODEL_DIR / name
        if target.is_file():
            with target.open("rb") as file:
                if hashlib.file_digest(file, "sha256").hexdigest() == expected:
                    continue
        temporary = target.with_name(f"{target.name}.{uuid.uuid4().hex}.tmp")
        try:
            digest = hashlib.sha256()
            url = f"https://huggingface.co/{REPO}/resolve/{REVISION}/{source}?download=true"
            with urlopen(url, timeout=60) as remote, temporary.open("wb") as file:
                while chunk := remote.read(1024 * 1024):
                    digest.update(chunk)
                    file.write(chunk)
            if digest.hexdigest() != expected:
                raise ValueError(f"Checksum mismatch for {name}")
            storage._replace(temporary, target)
            print(f"Installed {target}", flush=True)
        finally:
            temporary.unlink(missing_ok=True)


class QueryEncoder:
    def __init__(self):
        import numpy as np
        import onnxruntime as ort
        from tokenizers import Tokenizer

        self.np = np
        tokenizer = Tokenizer.from_file(str(MODEL_DIR / "tokenizer.json"))
        tokenizer.enable_truncation(max_length=192)
        tokenizer.enable_padding(pad_id=tokenizer.token_to_id("<pad>"), pad_token="<pad>")
        self.tokenizer = tokenizer
        options = ort.SessionOptions()
        options.intra_op_num_threads = 2
        options.inter_op_num_threads = 1
        self.model = ort.InferenceSession(
            str(MODEL_DIR / "model.onnx"), options, providers=["CPUExecutionProvider"]
        )
        self.input_names = {field.name for field in self.model.get_inputs()}

    def embed(self, texts):
        inputs = self.tokenizer.encode_batch(texts)
        np = self.np
        mask = np.array([row.attention_mask for row in inputs], dtype=np.int64)
        values = {
            "input_ids": np.array([row.ids for row in inputs], dtype=np.int64),
            "attention_mask": mask,
            "token_type_ids": np.array([row.type_ids for row in inputs], dtype=np.int64),
        }
        hidden = self.model.run(None, {name: values[name] for name in self.input_names})[0]
        mean = (hidden * mask[:, :, None]).sum(axis=1) / mask.sum(axis=1)[:, None]
        return mean / np.linalg.norm(mean, axis=1)[:, None]

    @lru_cache(maxsize=2)
    def catalog_vectors(self, catalog):
        batches = [self.embed(list(catalog[start:start + 8])) for start in range(0, len(catalog), 8)]
        return self.np.concatenate(batches)

    def similarities(self, query, games):
        ids = tuple(games)
        catalog = tuple(catalog_text(games[item]) for item in ids)
        query_vector = self.embed(["query: " + query])[0]
        values = self.catalog_vectors(catalog) @ query_vector
        return dict(zip(ids, map(float, values), strict=True))


def catalog_text(game):
    tags = [TAGS[tag][0] for tag in game.get("tag_ids") or [] if tag in TAGS]
    description = re.sub(r"\s+", " ", game.get("description") or "")
    return f"passage: {game['title']}. Steam tags: {', '.join(tags)}. {description}"


@lru_cache(maxsize=1)
def encoder():
    if not MODEL_DIR.joinpath("model.onnx").is_file():
        return None
    try:
        return QueryEncoder()
    except (ImportError, OSError, RuntimeError, ValueError):
        logger.exception("Could not load the local query encoder")
        return None


def score(query, games):
    if not games:
        return None
    try:
        model = encoder()
        return model.similarities(query, games) if model else None
    except Exception:
        logger.exception("Query model inference failed; using the explicit query rules")
        return None


def close_matches(similarities):
    if not similarities:
        return {}
    best = max(similarities.values())
    if best < MIN_BEST_SIMILARITY or best - median(similarities.values()) < MIN_SIMILARITY_SPREAD:
        return {}
    return {
        item: similarity
        for item, similarity in similarities.items()
        if similarity >= max(MIN_BEST_SIMILARITY, best - NEAR_MATCH_WINDOW)
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    install()
