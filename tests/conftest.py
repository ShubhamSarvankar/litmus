"""Test sessions run with no minimum part size: most tests upload parts of a few bytes
that AWS would refuse with EntityTooSmall. Set before server.config is imported, since
the settings are read once. The tests of the minimum itself name theirs through
create_app."""

import os

os.environ["S3_MIN_PART_SIZE"] = "0"
