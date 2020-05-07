import traceback

import csv
from gzip import GzipFile
from tempfile import NamedTemporaryFile

from checkdt.helpers.s3_utils import S3Conn


def csv_to_s3(cursor, s3_bucket, s3_key):
    csv_file = NamedTemporaryFile(delete=False)
    with GzipFile(fileobj=csv_file) as gzipped_file:
        writer = csv.writer(gzipped_file)
        for row in cursor:
            writer.write_row(row)

    csv_file.seek(0)

    try:
        uploaded_key = S3Conn(s3_bucket).upload_file(csv_file.name, s3_key)
        return uploaded_key
    except Exception:
        traceback.print_exc()