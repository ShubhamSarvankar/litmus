"""Each rule of server/bucket_names.py, accepted and rejected.

Rules: https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
"""

import pytest

from server.bucket_names import bucket_name_violation


def _rejected(name: str, fragment: str) -> None:
    violation = bucket_name_violation(name)
    assert violation is not None, name
    assert fragment in violation, (name, violation)


@pytest.mark.parametrize("length", [3, 4, 62, 63])
def test_length_accepted(length):
    assert bucket_name_violation("a" * length) is None


@pytest.mark.parametrize("name", ["", "a", "ab", "a" * 64, "a" * 255])
def test_length_rejected(name):
    _rejected(name, "length")


@pytest.mark.parametrize(
    "name", ["abc", "a1b", "123", "my-bucket", "my.bucket", "a-b.c-d", "0abc9", "a--b", "a.b.c"]
)
def test_characters_accepted(name):
    assert bucket_name_violation(name) is None


@pytest.mark.parametrize(
    "name",
    [
        "foo_bar",
        "_abc",
        "FooBar",
        "fooBar",
        "foo bar",
        "foo/bar",
        "foo:bar",
        "foo+bar",
        "fooé",
        "foo\n",
    ],
)
def test_characters_rejected(name):
    _rejected(name, "only lowercase")


@pytest.mark.parametrize("name", ["a1b", "0ab", "ab9", "9ab9"])
def test_edges_accepted(name):
    assert bucket_name_violation(name) is None


@pytest.mark.parametrize("name", ["-abc", "abc-", ".abc", "abc.", "foo-", "-foo", "a.-"])
def test_edges_rejected(name):
    _rejected(name, "begin and end")


@pytest.mark.parametrize("name", ["a.b", "a.b.c.d.e", "a-b.c"])
def test_single_periods_accepted(name):
    assert bucket_name_violation(name) is None


@pytest.mark.parametrize("name", ["foo..bar", "a..b", "foo...bar"])
def test_adjacent_periods_rejected(name):
    _rejected(name, "adjacent periods")


@pytest.mark.parametrize("name", ["foo.-bar", "foo-.bar"])
def test_period_next_to_hyphen_is_valid_under_aws(name):
    # The AWS page has no rule against "." beside "-", so these are accepted. The ceph
    # dns_dot_dash and dns_dash_dot tests expect 400 for them; Litmus follows AWS and
    # leaves those two failing (docs/compatibility.md).
    assert bucket_name_violation(name) is None


@pytest.mark.parametrize("name", ["192.168.5.4", "192.168.5.123", "0.0.0.0", "999.999.999.999"])
def test_ip_shape_rejected(name):
    _rejected(name, "IP address")


@pytest.mark.parametrize(
    "name", ["192.168.5", "192.168.5.4.5", "192.168.5.x", "a.168.5.4", "192.168.5.4a", "1.2.3"]
)
def test_not_ip_shaped_accepted(name):
    assert bucket_name_violation(name) is None


@pytest.mark.parametrize("prefix", ["xn--", "sthree-", "amzn-s3-demo-"])
def test_reserved_prefix_rejected(prefix):
    _rejected(prefix + "bucket", f"start with {prefix}")


@pytest.mark.parametrize(
    "name", ["xn-bucket", "xn-abc", "sthree", "xsthree-bucket", "amzn-s3-demo"]
)
def test_near_prefix_accepted(name):
    assert bucket_name_violation(name) is None


@pytest.mark.parametrize("suffix", ["-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3", "-an"])
def test_reserved_suffix_rejected(suffix):
    _rejected("bucket" + suffix, f"end with {suffix}")


@pytest.mark.parametrize(
    "name",
    ["s3alias", "bucket-s3alia", "bucket.map", "bucket-x-s3", "bucket-a", "bucket-and", "bucketan"],
)
def test_near_suffix_accepted(name):
    assert bucket_name_violation(name) is None


@pytest.mark.parametrize("name", [".", ".."])
def test_dot_segments_rejected(name):
    assert bucket_name_violation(name) is not None


def test_ceph_prefix_names_accepted():
    assert bucket_name_violation("litmus-test-123") is None
    assert bucket_name_violation("0litmus-test-foo") is None
    assert bucket_name_violation("alitmus-test-foo") is None
