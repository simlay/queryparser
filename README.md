[![codecov](https://codecov.io/gh/simlay/queryparser/graph/badge.svg?token=3ZHNK9NGXR)](https://codecov.io/gh/simlay/queryparser)
# A Query Parser Exercise

The prompt for this exercise is in rust_code_challenge.pdf.

# Run and testing
* `cargo run` will run using the sql found in `queries.sql` and metadata from
`columns.csv`. This is configurable.
* `cargo test` will test against a couple of examples.

# Output
The spec for this exercise didn't really have a goal output. So, `cargo run`
prints the lineage and the opacity of that lineage for each sql statement in
`queries.sql`.

## Status
- [x] simple select with identifiers and simple functions (like `COUNT` ) only
- [x] column usage
- [x] unions
- [x] handling column aliases
- [ ] nested queries
- [ ] handling expressions with multiple source columns (like + or functions)
