import redis
import json
import pickle
import hashlib
from os.path import join
from umake.cache.base_cache import Cache, MetadataCache
from umake.config import global_config
from umake.colored_output import out


class RedisCache(Cache):

    def __init__(self):
        self._redis = redis.Redis(host=global_config.remote_hostname,
                                  port=global_config.remote_port,
                                  db=0)

    def open_cache(self, cache_hash)->MetadataCache:
        try:
            data = self._redis.hget(cache_hash.hex(), 'md')
            if data is None:
                raise FileNotFoundError(f"No data for {cache_hash.hex()} in redis")
            return pickle.loads(data)
        except redis.TimeoutError:
            self._increase_timeout_and_check()
        except redis.RedisError:
            out.print_fail("Critical redis error detected!")
            global_config.remote_cache_enable = False

    def save_cache(self, cache_hash, metadata_cache: MetadataCache):
        md = pickle.dumps(metadata_cache, protocol=pickle.HIGHEST_PROTOCOL)
        try:
            self._redis.hset(cache_hash.hex(), 'md', md)
        except redis.TimeoutError:
            self._increase_timeout_and_check()
        except redis.RedisError:
            out.print_fail("Critical redis error detected!")
            global_config.remote_cache_enable = False

    def _get_cache(self, deps_hash, targets):
        if deps_hash is None:
            return False

        cache_src = deps_hash.hex()
        try:
            for target in targets:
                f = hashlib.sha1(target.encode("ascii")).hexdigest()
                src = join(cache_src, f)

                # Try to get the file data from redis
                file_attr = self._redis.hget(src, 'metadata')
                if file_attr is None:
                    return False
                file_attr = json.loads(file_attr)

                file_data = self._redis.hget(src, 'file_data')
                if file_data is None:
                    return False

                with open(target, 'wb') as target_file:
                    target_file.write(file_data)

                st_mode = int(file_attr["st_mode"])
                self._set_chmod(target, st_mode)
        except redis.TimeoutError:
            self._increase_timeout_and_check()
            return False
        except redis.RedisError:
            out.print_fail("Critical redis error detected!")
            global_config.remote_cache_enable = False
            return False

        return True


    def _save_cache(self, deps_hash, targets):
        cache_dst = deps_hash.hex()
        try:
            for target in targets:
                dst = join(cache_dst, hashlib.sha1(target.encode("ascii")).hexdigest())
                file_attr = {"st_mode": self._get_chmod(target)}
                with open(target, 'rb') as target_data:
                    self._redis.hset(dst, 'file_data', target_data.read())
                    self._redis.hset(dst, 'metadata', json.dumps(file_attr))
        except redis.TimeoutError:
            self._increase_timeout_and_check()
        except redis.RedisError:
            out.print_fail("Critical redis error detected!")
            global_config.remote_cache_enable = False

    def get_cache_stats(self):
        pass

    def clear_bucket(self):
        """
        We assume that all keys in the given database are umake cache.
        Thus we need to flush everything.
        """
        self._redis.flushdb()