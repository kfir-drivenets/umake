import os
from stat import S_IMODE
from abc import ABC, abstractmethod
from umake.colored_output import out
from umake.config import global_config


class MetadataCache:
    def __init__(self, deps):
        self.deps = deps


class Cache(ABC):

    def __init__(self):
        self.n_timeouts = 0

    def _increase_timeout_and_check(self):
        self.n_timeouts += 1
        if self.n_timeouts >= 3:
            out.print_fail(f"remote cache timedout {self.n_timeouts} time, disabling remote cahce")
            global_config.remote_cache_enable = False

    def _get_chmod(self, src):
        if hasattr(os, 'chmod'):
            st = os.stat(src)
            return st.st_mode
        else:
            return None

    def _set_chmod(self, dst, st_mode):
        os.chmod(dst, S_IMODE(st_mode))

    @abstractmethod
    def open_cache(self, cache_hash)->MetadataCache:
        """
        Get an object from the cache using a given hash.

        :param cache_hash: The key of the object to get
        :return: The resulting element from cache.
        """
        pass

    @abstractmethod
    def save_cache(self, cache_hash, metadata_cache: MetadataCache):
        """
        Save a given object into the cache.

        :param cache_hash:     The hash to use for storing the element in the cache.
        :param metadata_cache: The object to store in the cache.
        """
        pass

    @abstractmethod
    def get_cache_stats(self):
        """
        Get stats on the number of artifacts in the cache.
        """
        pass

    @abstractmethod
    def clear_bucket(self):
        """
        Delete all the artifacts in the cache.
        Mainly used for clean variants.
        """
        pass