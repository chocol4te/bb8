//! Redis support for the `bb8` connection pool.
#![deny(missing_docs, missing_debug_implementations)]

pub extern crate bb8;
pub extern crate redis;

extern crate futures;
extern crate tokio_core;

use futures::Future;
use tokio_core::reactor::Handle;

use std::io;
use std::option::Option;

type Result<T> = std::result::Result<T, redis::RedisError>;

/// A `bb8::ManageConnection` for `redis::async::Connection`s.
#[derive(Clone, Debug)]
pub struct RedisConnectionManager {
    client: redis::Client,
}

impl RedisConnectionManager {
    /// Create a new `PostgresConnectionManager`.
    pub fn new(client: redis::Client) -> Result<RedisConnectionManager> {
        Ok(RedisConnectionManager { client })
    }
}

impl bb8::ManageConnection for RedisConnectionManager {
    type Connection = Option<redis::async::Connection>;
    type Error = redis::RedisError;

    fn connect(
        &self,
        _handle: Handle,
    ) -> Box<Future<Item = Self::Connection, Error = Self::Error> + 'static> {
        Box::new(self.client.get_async_connection().map(|conn| Some(conn)))
    }

    fn is_valid(
        &self,
        conn: Self::Connection,
    ) -> Box<Future<Item = Self::Connection, Error = (Self::Error, Self::Connection)>> {
        // The connection should only be None after a failure.
        Box::new(
            redis::cmd("PING")
                .query_async(conn.unwrap())
                .map_err(|err| (err, None))
                .map(|(conn, ())| Some(conn)),
        )
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.is_none()
    }

    fn timed_out(&self) -> Self::Error {
        io::Error::new(io::ErrorKind::TimedOut, "RedisConnectionManager timed out").into()
    }
}
