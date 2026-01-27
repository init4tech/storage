use signet_zenith::{
    Passage::{Enter, EnterToken},
    Transactor::Transact,
    Zenith::BlockHeader,
};

/// Newtype for [`BlockHeader`] that implements [`Compress`] and [`Decompress`].
///
/// This is an implementation detail of the [`ZenithHeaders`] table, and should
/// not be used outside the DB module.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct DbZenithHeader(pub BlockHeader);

impl From<BlockHeader> for DbZenithHeader {
    fn from(header: BlockHeader) -> Self {
        Self(header)
    }
}

impl From<DbZenithHeader> for BlockHeader {
    fn from(header: DbZenithHeader) -> Self {
        header.0
    }
}

/// Newtype for extracted Signet events that implements [`Compress`] and
/// [`Decompress`].
///
/// This is an implementation detail of the [`SignetEvents`] table, and should
/// not be used outside the DB module.
///
/// Each event is stored as a separate entry in the same table.
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
