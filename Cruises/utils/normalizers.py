# utils/normalizers.py
# En tu normalizer:
import re
import unicodedata


def clean_column_name(name):
    name = str(name)
    name = re.sub(r'[#\s]+', '_', name)
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii')
    name = re.sub(r'[^\w_]', '', name)
    name = name.strip('_').lower()
    name = re.sub(r'_+', '_', name)
    return name
