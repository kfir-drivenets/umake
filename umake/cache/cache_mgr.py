import importlib
from enum import IntEnum
from .base_cache import MetadataCache
from .fs_cache import FsCache
from ..config import global_config

# Note that we don't import remote caches by default since they can be heavy

class CacheMgr:

    class CacheType(IntEnum):
        NOT_CACHED = 0
        LOCAL = 1
        REMOTE = 2

    fs_cache: FsCache = FsCache()
    def __init__(self):
        if global_config.remote_cache_enable:
            if global_config.remote_cache_type == 'minio':
                minio_cache = importlib.import_module("umake.cache.minio_cache")
                self.remote_cache = minio_cache.MinioCache()
            elif global_config.remote_cache_type == 'redis':
                redis_cache = importlib.import_module("umake.cache.redis_cache")
                self.remote_cache = redis_cache.RedisCache()

    def open_cache(self, cache_hash) -> MetadataCache:
        try:
            if global_config.local_cache:
                return self.fs_cache.open_cache(cache_hash)
            else:
                raise FileNotFoundError
        except FileNotFoundError:
            if global_config.remote_cache_enable:
                return self.remote_cache.open_cache(cache_hash)
            raise FileNotFoundError

    def save_cache(self, cache_hash, metadata_cache: MetadataCache):
        if global_config.local_cache:
            self.fs_cache.save_cache(cache_hash, metadata_cache)
        if global_config.remote_cache_enable and global_config.remote_write_enable:
            self.remote_cache.save_cache(cache_hash, metadata_cache)

    def _get_cache(self, deps_hash, targets):
        ret = False
        if global_config.local_cache:
            ret = self.fs_cache._get_cache(deps_hash, targets)
        if ret is False:
            if global_config.remote_cache_enable:
                ret = self.remote_cache._get_cache(deps_hash, targets)
                if ret is True:
                    return CacheMgr.CacheType.REMOTE
        else:
            return CacheMgr.CacheType.LOCAL
        return CacheMgr.CacheType.NOT_CACHED

    def _save_cache(self, deps_hash, targets, local_only=False):
        if global_config.local_cache:
            self.fs_cache._save_cache(deps_hash, targets)
        if local_only:
            return
        if global_config.remote_cache_enable and global_config.remote_write_enable:
            self.remote_cache._save_cache(deps_hash, targets)

    def gc(self):
        self.fs_cache.gc()
