#[cfg(test)]
use core::str::FromStr;
use csv::{Error, ReaderBuilder, Trim};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::env;

type ClientId = u16;
type TxId = u32;

// Client information consists of their available and held funds
// and information whether the client is locked.
// Total funds are not stored, since they can be trivially calculated
// from available + held.
#[derive(Debug, Clone)]
struct ClientInfo {
    available: Decimal,
    held: Decimal,
    locked: bool,
}

impl ClientInfo {
    fn new(amount: Decimal) -> Self {
        Self {
            available: amount,
            held: Decimal::new(0, 0),
            locked: false,
        }
    }

    fn total(&self) -> Decimal {
        self.available + self.held
    }
}

// There's no specific type associated with deposit or withdrawal,
// because it only introduces complications in the type system and no gains:
// the transaction simply uses positive/negative amounts
struct Transaction {
    client: ClientId,
    amount: Decimal,
}

#[derive(Default)]
struct TransactionEngine {
    // Performance note: if the number of clients is expected to be close to u16::MAX,
    // it's better to use a flat array of clients, which is more cache-friendly
    // and allows much faster lookups. This optimization should only be applied
    // if looking up clients shows up as a bottleneck in microbenchmarks
    clients: HashMap<ClientId, ClientInfo>,
    transactions: HashMap<TxId, Transaction>,
    disputed: HashSet<TxId>,
}

// Transaction engine capable of serving deposits, withdrawals, disputes,
// resolves and chargebacks
impl TransactionEngine {
    fn new() -> Self {
        Default::default()
    }

    // Performance note: this validation uses extra lookups in the transactions and
    // client maps, but also makes the code clearer to read. In case of a bottleneck,
    // all checks can be inlined to respective functions.
    fn valid(&self, op: &str, tx: TxId, client: ClientId, amount: Decimal) -> bool {
        let should_exist: bool = !matches!(op, "deposit" | "withdrawal");
        let exist_check = match self.transactions.get(&tx) {
            Some(_) => {
                if !should_exist {
                    eprintln!("Transaction {} already exists", tx)
                }
                should_exist
            }
            None => {
                if should_exist {
                    eprintln!("Transaction {} does not exist", tx)
                }
                !should_exist
            }
        };
        if !exist_check {
            return false;
        }
        if amount.is_sign_negative() {
            eprintln!("Invalid negative amount for deposit: {}", amount);
            return false;
        }
        if let Some(info) = self.clients.get(&client) {
            if info.locked {
                eprintln!("Client {} locked", client);
                return false;
            }
        }
        true
    }

    // Deposits funds
    fn deposit(&mut self, tx: TxId, client: ClientId, amount: Decimal) {
        self.transactions.insert(tx, Transaction { client, amount });

        if let Some(info) = self.clients.get_mut(&client) {
            info.available += amount;
        } else {
            self.clients.insert(client, ClientInfo::new(amount));
        }
    }

    // Withdraws funds if possible; the operation is ignored if no sufficient
    // funds are available
    fn withdraw(&mut self, tx: TxId, client: ClientId, amount: Decimal) {
        let mut amount = amount;
        amount.set_sign_negative(true);
        self.transactions.insert(tx, Transaction { client, amount });

        if let Some(info) = self.clients.get_mut(&client) {
            if info.available + amount >= 0.into() {
                info.available += amount;
            } else {
                eprintln!(
                    "Not enough funds {} for withdrawing {}",
                    info.available, amount
                );
                self.transactions.remove(&tx);
            }
        } else {
            eprintln!("No such client: {}", client);
            self.transactions.remove(&tx);
        }
    }

    // Handles a dispute, moving funds into `held`
    // NOTE: disputing a withdrawal is not specified, so the semantics
    // are assumed as follows: it's legal to dispute, resolve and chargeback
    // withdrawals, but available funds may never go below zero, or the operation
    // is dropped as invalid. Alternatively, withdrawal disputes could be banned,
    // which is trivial to validate.
    fn dispute(&mut self, tx: TxId, client: ClientId) {
        if self.disputed.contains(&tx) {
            eprintln!("Transaction already disputed");
            return;
        }
        self.disputed.insert(tx);
        if let Some(tx_entry) = self.transactions.get(&tx) {
            if tx_entry.client != client {
                eprintln!(
                    "Disputed transaction {} doesn't match the client id {}, skipping",
                    tx, client
                );
                return;
            }
            if let Some(info) = self.clients.get_mut(&client) {
                let amount = tx_entry.amount;
                if amount > info.available {
                    eprintln!(
                        "Disputed amount {} larger than available funds: {}, skipping",
                        amount, info.available
                    );
                    return;
                }
                info.available -= amount;
                info.held += amount;
            }
        }
    }

    // Resolves a dispute, moving funds from `held` back into `available`
    fn resolve(&mut self, tx: TxId, client: ClientId) {
        if !self.disputed.contains(&tx) {
            eprintln!("Transaction not disputed");
            return;
        }
        self.disputed.remove(&tx);

        if let Some(tx_entry) = self.transactions.get(&tx) {
            if tx_entry.client != client {
                eprintln!(
                    "Resolved transaction {} doesn't match the client id {}, skipping",
                    tx, client
                );
                return;
            }
            if let Some(info) = self.clients.get_mut(&client) {
                let amount = tx_entry.amount;
                if amount > info.held {
                    eprintln!(
                        "Resolved amount {} larger than held funds: {}, skipping",
                        amount, info.held
                    );
                    return;
                }
                info.available += amount;
                info.held -= amount;
            }
        }
    }

    // Charges back a dispute, removing funds from `held` and locking the account
    fn chargeback(&mut self, tx: TxId, client: ClientId) {
        if !self.disputed.contains(&tx) {
            eprintln!("Transaction not disputed");
            return;
        }
        self.disputed.remove(&tx);

        if let Some(tx_entry) = self.transactions.get(&tx) {
            if tx_entry.client != client {
                eprintln!(
                    "Charged-back transaction {} doesn't match the client id {}, skipping",
                    tx, client
                );
                return;
            }
            if let Some(info) = self.clients.get_mut(&client) {
                let amount = tx_entry.amount;
                if amount > info.held {
                    eprintln!(
                        "Charged-back amount {} larger than held funds: {}, skipping",
                        amount, info.held
                    );
                    return;
                }
                info.held -= amount;
                info.locked = true;
            }
        }
    }

    fn from_csv_reader<R: std::io::Read>(mut reader: csv::Reader<R>) -> Result<Self, Box<Error>> {
        let mut engine = Self::new();

        for row in reader.deserialize::<Row>() {
            match row {
                Ok(row) => {
                    let amount = row.amount.unwrap_or_else(|| 0.into());
                    if !engine.valid(&row.op, row.tx, row.client, amount) {
                        continue;
                    }
                    match row.op.as_str() {
                        "deposit" => engine.deposit(row.tx, row.client, amount),
                        "withdrawal" => engine.withdraw(row.tx, row.client, amount),
                        "dispute" => engine.dispute(row.tx, row.client),
                        "resolve" => engine.resolve(row.tx, row.client),
                        "chargeback" => engine.chargeback(row.tx, row.client),
                        _ => eprintln!("Unknown transaction type {}", row.op),
                    }
                },
                Err(e) => eprintln!("Invalid row: {}", e),
            }
        }
        Ok(engine)
    }

    fn from_csv(path: &str) -> Result<Self, Box<Error>> {
        let reader = ReaderBuilder::new().trim(Trim::All).flexible(true).from_path(path)?;

        Self::from_csv_reader(reader)
    }

    fn to_csv(&self) {
        println!("client,available,held,total,locked");
        self.clients
            .iter()
            .map(|(id, info)| {
                println!(
                    "{},{},{},{},{}",
                    id,
                    info.available,
                    info.held,
                    info.total(),
                    info.locked
                )
            })
            .collect()
    }

    #[cfg(test)]
    fn clients(&self) -> &HashMap<ClientId, ClientInfo> {
        &self.clients
    }
}

#[derive(Debug, Deserialize)]
struct Row {
    #[serde(rename = "type")]
    op: String,
    client: ClientId,
    tx: TxId,
    amount: Option<Decimal>,
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        println!("Usage: {} path-to-csv", &args[0]);
        std::process::exit(1);
    }
    let path = &args[1];

    match TransactionEngine::from_csv(&path) {
        Ok(engine) => engine.to_csv(),
        Err(e) => {
            eprintln!("{}", e);
            std::process::exit(1)
        }
    }
}

#[cfg(test)]
fn test_clients(input: &str) -> HashMap<ClientId, ClientInfo> {
    let reader = ReaderBuilder::new()
        .flexible(true)
        .from_reader(input.as_bytes());
    let engine = TransactionEngine::from_csv_reader(reader).unwrap();
    let clients = engine.clients();
    clients.clone()
}

#[test]
fn test_deposit_duplicated() {
    let input = r#"type,client,tx,amount
deposit,1,1,1.0
deposit,1,1,1.0
deposit,2,1,1.0
deposit,3,1,1.0
"#;
    let clients = test_clients(&input);
    assert_eq!(clients.len(), 1);
    let client = clients.get(&1).unwrap();
    assert_eq!(client.available, 1.into());
    assert_eq!(client.locked, false);
}

#[test]
fn test_withdraw() {
    let input = r#"type,client,tx,amount
deposit,1,1,5.0
withdrawal,1,2,7.0
withdrawal,1,3,5.1
withdrawal,1,4,5.5
"#;
    let clients = test_clients(&input);
    assert_eq!(clients.len(), 1);
    let client = clients.get(&1).unwrap();
    assert_eq!(client.available, 5.into());
    assert_eq!(client.locked, false);
}

#[test]
fn test_dispute_unresolved() {
    let input = r#"type,client,tx,amount
deposit,1,1,5.0
deposit,1,2,3.0
deposit,1,3,4.0
deposit,1,4,1.0
dispute,1,4,
dispute,1,2,
"#;
    let clients = test_clients(&input);
    assert_eq!(clients.len(), 1);
    let client = clients.get(&1).unwrap();
    assert_eq!(client.available, 9.into());
    assert_eq!(client.held, 4.into());
    assert_eq!(client.locked, false);
}

#[test]
fn test_dispute_resolve() {
    let input = r#"type,client,tx,amount
deposit,1,1,5.0
deposit,1,2,3.0
deposit,1,3,4.0
deposit,1,4,1.0
dispute,1,4,
dispute,1,2,
resolve,1,4,
"#;
    let clients = test_clients(&input);
    assert_eq!(clients.len(), 1);
    let client = clients.get(&1).unwrap();
    assert_eq!(client.available, 10.into());
    assert_eq!(client.held, 3.into());
    assert_eq!(client.locked, false);
}

#[test]
fn test_dispute_chargeback() {
    let input = r#"type,client,tx,amount
deposit,1,1,5.0
deposit,1,2,3.0
deposit,1,3,4.0
deposit,1,4,1.0
dispute,1,4,
dispute,1,2,
chargeback,1,4,
"#;
    let clients = test_clients(&input);
    assert_eq!(clients.len(), 1);
    let client = clients.get(&1).unwrap();
    assert_eq!(client.available, 9.into());
    assert_eq!(client.held, 3.into());
    assert_eq!(client.locked, true);
}

#[test]
fn test_locked() {
    let input = r#"type,client,tx,amount
deposit,1,1,5.0
deposit,1,2,3.0
deposit,1,3,4.0
deposit,1,4,1.0
dispute,1,4,
dispute,1,2,
chargeback,1,4,
deposit,1,7,100
deposit,1,8,15
withdrawal,1,9,7
dispute,1,9,
chargeback,1,9
"#;
    let clients = test_clients(&input);
    assert_eq!(clients.len(), 1);
    let client = clients.get(&1).unwrap();
    assert_eq!(client.available, 9.into());
    assert_eq!(client.held, 3.into());
    assert_eq!(client.locked, true);
}

#[test]
fn test_multiple_clients() {
    let input = r#"type,client,tx,amount
deposit,1,1,5.0
deposit,2,2,3.0
withdrawal,1,3,1.3
withdrawal,1,5,1.1
deposit,3,4,7.0
"#;
    let clients = test_clients(&input);
    assert_eq!(clients.len(), 3);
    let client1 = clients.get(&1).unwrap();
    let client2 = clients.get(&2).unwrap();
    let client3 = clients.get(&3).unwrap();
    assert_eq!(client1.available, Decimal::from_str("2.6").unwrap());
    assert_eq!(client2.available, 3.into());
    assert_eq!(client3.available, 7.into());
}

#[test]
fn test_precision() {
    let input = r#"type,client,tx,amount
deposit,1,1,5.1234
deposit,1,2,1.4321
withdrawal,1,3,1.1111
"#;
    let clients = test_clients(&input);
    assert_eq!(clients.len(), 1);
    let client = clients.get(&1).unwrap();
    assert_eq!(client.available, Decimal::from_str("5.4444").unwrap());
    assert_eq!(client.held, 0.into());
    assert_eq!(client.locked, false);
}

#[test]
fn test_dispute_unresolved_withdrawal() {
    let input = r#"type,client,tx,amount
deposit,1,1,5.0
deposit,1,2,3.0
deposit,1,3,4.0
withdrawal,1,4,8.0
dispute,1,4,
dispute,1,2,
"#;
    let clients = test_clients(&input);
    assert_eq!(clients.len(), 1);
    let client = clients.get(&1).unwrap();
    assert_eq!(client.available, 9.into());
    assert_eq!(client.held, Decimal::from_str("-5").unwrap());
    assert_eq!(client.locked, false);
}

#[test]
fn test_dispute_resolve_withdrawal() {
    let input = r#"type,client,tx,amount
deposit,1,1,5.0
deposit,1,2,3.0
deposit,1,3,4.0
withdrawal,1,4,8.0
dispute,1,4,
dispute,1,2,
resolve,1,4,
"#;
    let clients = test_clients(&input);
    assert_eq!(clients.len(), 1);
    let client = clients.get(&1).unwrap();
    assert_eq!(client.available, 1.into());
    assert_eq!(client.held, 3.into());
    assert_eq!(client.locked, false);
}

#[test]
fn test_dispute_chargeback_withdrawal() {
    let input = r#"type,client,tx,amount
deposit,1,1,5.0
deposit,1,2,3.0
deposit,1,3,4.0
withdrawal,1,4,8.0
dispute,1,4,
dispute,1,2,
chargeback,1,4,
"#;
    let clients = test_clients(&input);
    assert_eq!(clients.len(), 1);
    let client = clients.get(&1).unwrap();
    assert_eq!(client.available, 9.into());
    assert_eq!(client.held, 3.into());
    assert_eq!(client.locked, true);
}

#[test]
fn test_incorrect_ops() {
    let input = r#"type,client,tx,amount
deposits,1,1,5.0
withdraw,2,2,7.0
withdrawx,3,3,5.1
disputer,4,4,5.5
resolv,4,4,5.5
charge-back,4,4,5.5
,,,
"#;
    let clients = test_clients(&input);
    assert_eq!(clients.len(), 0);
}

#[test]
fn test_undisputed() {
    let input = r#"type,client,tx,amount
deposit,1,1,1.0
resolve,1,2,1.0
chargeback,2,3,1.0
resolve,2,4,
chargeback,2,5,
"#;
    let clients = test_clients(&input);
    assert_eq!(clients.len(), 1);
    let client = clients.get(&1).unwrap();
    assert_eq!(client.available, 1.into());
    assert_eq!(client.locked, false);
}

#[test]
fn test_withdraw_deposit() {
    let input = r#"type,client,tx,amount
withdrawal,1,1,5.01
deposit,1,2,7.01
"#;
    let clients = test_clients(&input);
    assert_eq!(clients.len(), 1);
    let client = clients.get(&1).unwrap();
    assert_eq!(client.available, Decimal::from_str("7.01").unwrap());
    assert_eq!(client.held, 0.into());
    assert_eq!(client.locked, false);
}

#[test]
fn test_dispute_not_applied() {
    let input = r#"type,client,tx,amount
withdrawal,1,1,5.01
deposit,1,2,7.01
dispute,1,1,
"#;
    let clients = test_clients(&input);
    assert_eq!(clients.len(), 1);
    let client = clients.get(&1).unwrap();
    assert_eq!(client.available, Decimal::from_str("7.01").unwrap());
    assert_eq!(client.held, 0.into());
    assert_eq!(client.locked, false);
}

#[test]
fn test_dispute_not_applied_client_exists() {
    let input = r#"type,client,tx,amount
deposit,1,4,3.0013
withdrawal,1,1,5.01
deposit,1,2,7.01
dispute,1,1,
"#;
    let clients = test_clients(&input);
    assert_eq!(clients.len(), 1);
    let client = clients.get(&1).unwrap();
    assert_eq!(client.available, Decimal::from_str("10.0113").unwrap());
    assert_eq!(client.held, 0.into());
    assert_eq!(client.locked, false);
}
