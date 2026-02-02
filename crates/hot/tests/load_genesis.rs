//! Integration tests for `load_genesis`.
//!
//! These tests load each genesis file from `tests/artifacts/` and verify
//! database correctness after loading.

use alloy::{
    genesis::Genesis,
    primitives::{U256, keccak256},
};
use signet_hot::{
    HotKv,
    db::{HistoryRead, HistoryWrite, HotDbRead, UnsafeDbWrite},
    mem::MemKv,
};
use signet_storage_types::EthereumHardfork;
use std::path::Path;

/// Load a genesis file from the given path.
fn load_genesis_file(path: &Path) -> Genesis {
    let content = std::fs::read_to_string(path).unwrap();
    serde_json::from_str(&content).unwrap()
}

/// Determine which hardforks are active at genesis based on the genesis config.
fn hardforks_for_genesis(genesis: &Genesis) -> EthereumHardfork {
    let config = &genesis.config;
    let genesis_time = genesis.timestamp;

    let mut hardforks = EthereumHardfork::Frontier;

    // Block-based hardforks at genesis (block 0)
    if config.homestead_block == Some(0) {
        hardforks |= EthereumHardfork::Homestead;
    }
    if config.dao_fork_block == Some(0) && config.dao_fork_support {
        hardforks |= EthereumHardfork::Dao;
    }
    if config.eip150_block == Some(0) {
        hardforks |= EthereumHardfork::Tangerine;
    }
    if config.eip155_block == Some(0) {
        hardforks |= EthereumHardfork::SpuriousDragon;
    }
    if config.byzantium_block == Some(0) {
        hardforks |= EthereumHardfork::Byzantium;
    }
    if config.constantinople_block == Some(0) {
        hardforks |= EthereumHardfork::Constantinople;
    }
    if config.petersburg_block == Some(0) {
        hardforks |= EthereumHardfork::Petersburg;
    }
    if config.istanbul_block == Some(0) {
        hardforks |= EthereumHardfork::Istanbul;
    }
    if config.muir_glacier_block == Some(0) {
        hardforks |= EthereumHardfork::MuirGlacier;
    }
    if config.berlin_block == Some(0) {
        hardforks |= EthereumHardfork::Berlin;
    }
    if config.london_block == Some(0) {
        hardforks |= EthereumHardfork::London;
    }
    if config.arrow_glacier_block == Some(0) {
        hardforks |= EthereumHardfork::ArrowGlacier;
    }
    if config.gray_glacier_block == Some(0) {
        hardforks |= EthereumHardfork::GrayGlacier;
    }
    if config.merge_netsplit_block == Some(0) {
        hardforks |= EthereumHardfork::Paris;
    }

    // Time-based hardforks at genesis timestamp
    if config.shanghai_time.is_some_and(|t| t <= genesis_time) {
        hardforks |= EthereumHardfork::Shanghai;
    }
    if config.cancun_time.is_some_and(|t| t <= genesis_time) {
        hardforks |= EthereumHardfork::Cancun;
    }
    if config.prague_time.is_some_and(|t| t <= genesis_time) {
        hardforks |= EthereumHardfork::Prague;
    }

    hardforks
}

/// Verify that genesis was loaded correctly into the database.
fn verify_genesis_loaded(db: &MemKv, genesis: &Genesis, hardforks: &EthereumHardfork) {
    let reader = db.reader().unwrap();

    let genesis_number = genesis.number.unwrap_or_default();

    // Verify chain tip
    let tip = reader.get_chain_tip().unwrap();
    assert!(tip.is_some(), "Chain tip should exist after loading genesis");
    let (tip_number, tip_hash) = tip.unwrap();
    assert_eq!(tip_number, genesis_number, "Chain tip number should match genesis number");

    // Verify genesis header
    let header = reader.get_header(genesis_number).unwrap();
    assert!(header.is_some(), "Genesis header should exist");
    let header = header.unwrap();
    assert_eq!(header.number, genesis_number, "Header number should match");
    assert_eq!(header.gas_limit, genesis.gas_limit, "Gas limit should match");
    assert_eq!(header.timestamp, genesis.timestamp, "Timestamp should match");

    // Verify header hash lookup works
    let header_number_by_hash = reader.get_header_number(&tip_hash).unwrap();
    assert_eq!(
        header_number_by_hash,
        Some(genesis_number),
        "Header number lookup by hash should work"
    );

    // Verify expected hardfork-dependent header fields
    if hardforks.contains(EthereumHardfork::London) {
        assert!(header.base_fee_per_gas.is_some(), "Base fee should be set for London");
    }
    if hardforks.contains(EthereumHardfork::Shanghai) {
        assert!(header.withdrawals_root.is_some(), "Withdrawals root should be set for Shanghai");
    }
    if hardforks.contains(EthereumHardfork::Cancun) {
        assert!(
            header.parent_beacon_block_root.is_some(),
            "Parent beacon block root should be set for Cancun"
        );
        assert!(header.blob_gas_used.is_some(), "Blob gas used should be set for Cancun");
        assert!(header.excess_blob_gas.is_some(), "Excess blob gas should be set for Cancun");
    }
    if hardforks.contains(EthereumHardfork::Prague) {
        assert!(header.requests_hash.is_some(), "Requests hash should be set for Prague");
    }

    // Verify accounts
    for (address, account) in &genesis.alloc {
        let db_account = reader.get_account(address).unwrap();
        assert!(db_account.is_some(), "Account {address} should exist");
        let db_account = db_account.unwrap();

        assert_eq!(db_account.balance, account.balance, "Balance mismatch for {address}");
        assert_eq!(
            db_account.nonce,
            account.nonce.unwrap_or_default(),
            "Nonce mismatch for {address}"
        );

        // Verify bytecode
        if let Some(code) = &account.code {
            let code_hash = keccak256(code);
            assert_eq!(
                db_account.bytecode_hash,
                Some(code_hash),
                "Bytecode hash mismatch for {address}"
            );

            let bytecode = reader.get_bytecode(&code_hash).unwrap();
            assert!(bytecode.is_some(), "Bytecode should exist for {address}");
            let bytecode = bytecode.unwrap();
            assert_eq!(bytecode.original_bytes(), *code, "Bytecode content mismatch for {address}");
        } else {
            assert_eq!(
                db_account.bytecode_hash, None,
                "Bytecode hash should be None for {address}"
            );
        }

        // Verify storage
        if let Some(storage) = &account.storage {
            for (slot, value) in storage {
                let slot_u256 = U256::from_be_bytes(**slot);
                let expected_value = U256::from_be_bytes(**value);

                let db_value = reader.get_storage(address, &slot_u256).unwrap();
                assert!(db_value.is_some(), "Storage slot {slot} should exist for {address}");
                assert_eq!(
                    db_value.unwrap(),
                    expected_value,
                    "Storage value mismatch for {address} slot {slot}"
                );

                // Verify storage history
                let storage_history =
                    reader.get_storage_history(address, slot_u256, u64::MAX).unwrap();
                assert!(
                    storage_history.is_some(),
                    "Storage history should exist for {address} slot {slot}"
                );
                let history_list = storage_history.unwrap();
                assert!(
                    history_list.iter().any(|bn| bn == genesis_number),
                    "Storage history should contain genesis block for {address} slot {slot}"
                );
            }
        }

        // Verify account history
        let account_history = reader.get_account_history(address, u64::MAX).unwrap();
        assert!(account_history.is_some(), "Account history should exist for {address}");
        let history_list = account_history.unwrap();
        assert!(
            history_list.iter().any(|bn| bn == genesis_number),
            "Account history should contain genesis block for {address}"
        );
    }
}

/// Run the full genesis load test for a given genesis file.
fn run_genesis_test(genesis_path: &str) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/artifacts").join(genesis_path);
    let genesis = load_genesis_file(&path);
    let hardforks = hardforks_for_genesis(&genesis);

    let db = MemKv::new();
    let writer = db.writer().unwrap();
    writer.load_genesis(&genesis, &hardforks).unwrap();
    writer.commit().unwrap();

    verify_genesis_loaded(&db, &genesis, &hardforks);
}

#[test]
fn test_load_local_genesis() {
    run_genesis_test("local.genesis.json");
}

#[test]
fn test_load_mainnet_genesis() {
    run_genesis_test("mainnet.genesis.json");
}

#[test]
fn test_load_parmigiana_genesis() {
    run_genesis_test("parmigiana.genesis.json");
}

#[test]
fn test_load_pecorino_genesis() {
    run_genesis_test("pecorino.genesis.json");
}
