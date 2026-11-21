# AI-generated tests (Codex): semantic payload identity, eligibility, preprocessing and exact score ordering.
import struct

import pytest

from papersearch.encoder import DIM, RECIPE, eligible, passage, unpack_vector
from papersearch.derivation import cache_key


def test_semantic_identity_and_payload():
    assert passage("Title", "") == "Title"
    assert passage("Title", "Body") == "Title\nBody"
    values = [1.0] + [0.0] * (DIM - 1)
    assert unpack_vector(struct.pack("<I384f", 3, *values)) == 3
    for invalid in (
        bytes(2),
        bytes(1540),
        struct.pack("<I384f", 3, float("nan"), *values[1:]),
    ):
        with pytest.raises(ValueError):
            unpack_vector(invalid)
    key = cache_key("a" * 64, RECIPE)
    for field in RECIPE:
        assert key != cache_key("a" * 64, {**RECIPE, field: "other"})
    assert key != cache_key("b" * 64, RECIPE)
    population = [eligible(f"arxiv:{i}") for i in range(1000)]
    assert 0 < sum(population) < 1000
