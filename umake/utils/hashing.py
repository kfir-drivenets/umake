import blake3


def get_cache_key(data):
    return blake3.blake3(data).hexdigest()


def get_hash(data):
    return blake3.blake3(data).digest()