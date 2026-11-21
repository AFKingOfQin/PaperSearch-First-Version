# AI-generated code (Codex): offline, pinned CPU encoding for passage and query retrieval.
import hashlib
import os
import math
import struct
import zlib
from pathlib import Path

KIND = "bge-onnx-cpu-v1"
MODEL = "BAAI/bge-small-en-v1.5"
REVISION = "5c38ec7c405ec4b44b94cc5a9bb96e735b38267a"
PREPROCESS = "title-lf-abstract-cls128-v1"
RECIPE = dict(
    kind=KIND, model_id=MODEL, model_revision=REVISION, preprocessing=PREPROCESS
)
DIM = 384
MAX_TOKENS = 128
QUERY_PREFIX = "Represent this sentence for searching relevant passages: "
FILES = {
    "model.onnx": "828e1496d7fabb79cfa4dcd84fa38625c0d3d21da474a00f08db0f559940cf35",
    "tokenizer.json": "d241a60d5e8f04cc1b2b3e9ef7a4921b27bf526d9f6050ab90f9267a1f9e5c66",
}


def passage(title, abstract):
    return title + "\n" + abstract if abstract else title


# The fixed membership rule also applies to new imports and revisions.
ELIGIBILITY = "CRC32(UTF-8 paper_id) modulo 100 < 5"
PAYLOAD_BYTES = 4 + DIM * 4


def eligible(paper):
    return zlib.crc32(paper.encode("utf-8")) % 100 < 5


def eligibility_sql(paper_column, kind):
    return f"(({kind}='{KIND}' AND MOD(CRC32({paper_column}),100)<5) OR ({kind}<>'{KIND}' AND {paper_column} LIKE 'fixture:%'))"


def unpack_vector(payload):
    if len(payload) != PAYLOAD_BYTES:
        raise ValueError("Invalid semantic vector width")
    length, *values = struct.unpack("<I384f", payload)
    if length < 2 or not math.isclose(sum(v * v for v in values), 1.0, abs_tol=1e-4):
        raise ValueError("Invalid semantic vector norm or token length")
    return length


class Encoder:
    def __init__(self, directory=None, threads=2):
        import numpy as np
        import onnxruntime as ort
        from tokenizers import Tokenizer

        directory = Path(
            directory
            or os.environ.get(
                "PAPERSEARCH_MODEL_DIR", "/artifacts/models/bge-small-en-v1.5"
            )
        )
        for name, checksum in FILES.items():
            with (directory / name).open("rb") as f:
                if hashlib.file_digest(f, "sha256").hexdigest() != checksum:
                    raise ValueError(f"Model checksum mismatch: {name}")
        os.environ["TOKENIZERS_PARALLELISM"] = "false"
        self.tokenizer = Tokenizer.from_file(str(directory / "tokenizer.json"))
        self.tokenizer.no_padding()
        self.tokenizer.no_truncation()
        if not 1 <= threads <= 8:
            raise ValueError("Expected 1–8 CPU threads")
        options = ort.SessionOptions()
        options.intra_op_num_threads = threads
        options.inter_op_num_threads = 1
        self.session = ort.InferenceSession(
            str(directory / "model.onnx"), options, providers=["CPUExecutionProvider"]
        )
        self.np = np

    def encode(self, texts, *, query=False):
        np = self.np
        if not 1 <= len(texts) <= 64:
            raise ValueError("Expected 1–64 texts")
        tokens = self.tokenizer.encode_batch(
            [QUERY_PREFIX + t if query else t for t in texts]
        )
        lengths = [len(t.ids) for t in tokens]
        width = min(max(lengths), MAX_TOKENS)
        ids = np.zeros((len(tokens), width), dtype=np.int64)
        mask = np.zeros_like(ids)
        for i, t in enumerate(tokens):
            row = t.ids
            if len(row) > width:
                row = row[: width - 1] + row[-1:]  # Preserve the final SEP.
            ids[i, : len(row)] = row
            mask[i, : len(row)] = 1
        (hidden,) = self.session.run(
            ["last_hidden_state"],
            dict(input_ids=ids, attention_mask=mask, token_type_ids=np.zeros_like(ids)),
        )
        cls = hidden[:, 0, :]
        vectors = (cls / np.linalg.norm(cls, axis=1, keepdims=True)).astype("<f4")
        if vectors.shape != (len(texts), DIM) or not np.isfinite(vectors).all():
            raise ValueError("Invalid model output")
        return vectors, lengths
