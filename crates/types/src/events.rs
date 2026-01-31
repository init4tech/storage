use signet_zenith::{
    Passage::{Enter, EnterToken},
    Transactor::Transact,
    Zenith,
};

/// Newtype for [`Zenith::BlockHeader`] stored in cold storage.
///
/// This is an implementation detail of the `ZenithHeaders` table, and should
/// not be used outside the cold-storage DB module.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct DbZenithHeader(pub Zenith::BlockHeader);

impl From<Zenith::BlockHeader> for DbZenithHeader {
    fn from(header: Zenith::BlockHeader) -> Self {
        Self(header)
    }
}

impl From<DbZenithHeader> for Zenith::BlockHeader {
    fn from(header: DbZenithHeader) -> Self {
        header.0
    }
}

/// Extracted Signet events, stored in cold storage.
///
/// This is an implementation detail of the `SignetEvents` table, and should
/// not generally be used outside the cold-storage DB module.
///
/// The first element of each event tuple is the event's order within all
/// events in the block.
///
/// The second element is the event itself.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub enum DbSignetEvent {
    /// Each Transact event is stored as a separate entry in the same table.
    Transact(u64, Transact),
    /// Each Enter event is stored as a separate entry in the same table.
    Enter(u64, Enter),
    /// Each EnterToken event is stored as a separate entry in the same table.
    EnterToken(u64, EnterToken),
}
