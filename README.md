# iavl-viewer

Examines an application.db and reports module sizes and module+prefix sizes.

It's assumed that the database is a leveldb used by a Cosmos-SDK blockchain.

## Build

To build the application:
`go build`

The built binary will be `./iavl-viewer`

## Usage

`./iavl-viewer [<db dir>] [<module of interest>]`

The default `<db dir>` is `./data/application.db`.

Only one module of interest can be provided. If not provided, all modules (the entire db) will be processed.
If a module of interest is provided (e.g. `marker`), only that module will be processed.

## Output

Output is four parts.
1. The leveldb stats
2. Log messages including examples of keys for each module and prefix.
3. A table with an entry for each module and prefix. Entries sorted by total size.
4. A table with an entry for each module. Entries sorted by total size.
