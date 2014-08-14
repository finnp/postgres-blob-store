# postgres-blob-store

This module tries to be a Postgres implementation of [abstract-blob-store](https://github.com/maxogden/abstract-blob-store).

Right now it doesn't use `bytea`, but is saving the binary files in `text` format as `base64`
Strings.

It is very experimental, since I have no clue about PostgresSQL.