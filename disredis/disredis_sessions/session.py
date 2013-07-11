import redis

from django.contrib.sessions.backends.base import SessionBase, CreateError
try:
    from django.utils.encoding import force_text
except ImportError:  # Backwards compatibility for Django <1.5
    from django.utils.encoding import force_unicode as force_text

from disredis.disredis_client.client import DisredisClient
from disredis.disredis_sessions import settings


# Avoid new redis connection on each request

if settings.SESSION_REDIS_SENTINEL_URLS is not None:
    redis_server = DisredisClient(settings.SESSION_REDIS_SENTINEL_URLS)


class SessionStore(SessionBase):
    """
    Implements Redis database session store.
    """
    def __init__(self, session_key=None):
        super(SessionStore, self).__init__(session_key)

        self.server = redis_server

    def load(self):
        try:
            session_data = self.server.get(
                self.get_real_stored_key(self._get_or_create_session_key())
            )
            return self.decode(force_text(session_data))
        except:
            self.create()
            return {}

    def exists(self, session_key):
        return self.server.exists(self.get_real_stored_key(session_key))

    def create(self):
        while True:
            self._session_key = self._get_new_session_key()

            try:
                self.save(must_create=True)
            except CreateError:
                continue

            self.modified = True
            return

    def save(self, must_create=False):
        if must_create and self.exists(self._get_or_create_session_key()):
            raise CreateError

        data = self.encode(self._get_session(no_load=must_create))
        if redis.VERSION[0] >= 2:
            self.server.setex(
                self.get_real_stored_key(self._get_or_create_session_key()),
                self.get_expiry_age(),
                data
            )
        else:
            self.server.set(
                self.get_real_stored_key(self._get_or_create_session_key()),
                data
            )
            self.server.expire(
                self.get_real_stored_key(self._get_or_create_session_key()),
                self.get_expiry_age()
            )

    def delete(self, session_key=None):
        if session_key is None:
            if self.session_key is None:
                return
            session_key = self.session_key
        try:
            self.server.delete(self.get_real_stored_key(session_key))
        except:
            pass

    def get_real_stored_key(self, session_key):
        """Return the real key name in redis storage
        @return string
        """
        prefix = settings.SESSION_REDIS_PREFIX
        if not prefix:
            return session_key
        return ':'.join([prefix, session_key])
