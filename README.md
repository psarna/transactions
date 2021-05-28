### Tested against
 - unit tests
 - large local file
 - large stream

### Checked with
 - cargo fmt
 - cargo clippy
 - cargo test
 - manual tests

### Assumptions
 - it's legal to dispute a withdrawal, but all operations, including resolve and chargeback, must leave `available` funds nonnegative; otherwise, a transaction is dropped

### Performance notes
 - if the number of clients is expected to be near `u16::MAX`, it's better to use a plain array instead of a hash map
 - same for transactions - 4M records is still something a server can usually handle
 - at first glance, deposit and withdrawal transactions could have a separate type in the Rust type system, but since they're symmetrical, withdrawals are represented simply as transactions with negative amounts - the underlying decimal type is capable of storing the sign anyway, and it makes the structure footprint smaller
 - total funds are not denormalized and stored in order to further minimize the memory footprint - total funds are trivially computable from `available` + `held`
