// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

/// Rust implementation of messages required for Rust <-> OCaml FFI communication.

use std::fmt;
use std::fmt::Debug;

use derive_builder::Builder;
use failure::Fail;
use ocaml_interop::OCamlError;
use serde::{Deserialize, Serialize};

use crypto::hash::{BlockHash, ChainId, ContextHash, HashType, OperationHash, ProtocolHash};
use tezos_messages::p2p::encoding::block_header::{display_fitness, Fitness};
use tezos_messages::p2p::encoding::prelude::{BlockHeader, Operation, OperationsForBlocksMessage, Path};

pub type RustBytes = Vec<u8>;

/// Genesis block information structure
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct GenesisChain {
    pub time: String,
    pub block: String,
    pub protocol: String,
}

/// Voted protocol overrides
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ProtocolOverrides {
    pub forced_protocol_upgrades: Vec<(i32, String)>,
    pub voted_protocol_overrides: Vec<(String, String)>,
}

/// Patch_context key json
#[derive(Clone, Serialize, Deserialize)]
pub struct PatchContext {
    pub key: String,
    pub json: String,
}

impl fmt::Debug for PatchContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(key: {}, json: {})", &self.key, &self.json)
    }
}

/// Test chain information
#[derive(Debug, Serialize, Deserialize)]
pub struct TestChain {
    pub chain_id: RustBytes,
    pub protocol_hash: RustBytes,
    pub expiration_date: String,
}

/// Holds configuration for ocaml runtime - e.g. arguments which are passed to ocaml and can be change in runtime
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TezosRuntimeConfiguration {
    pub log_enabled: bool,
    pub no_of_ffi_calls_treshold_for_gc: i32,
    pub debug_mode: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, Builder, PartialEq)]
pub struct ApplyBlockRequest {
    pub chain_id: ChainId,
    pub block_header: BlockHeader,
    pub pred_header: BlockHeader,
    pub max_operations_ttl: i32,
    pub operations: Vec<Vec<Operation>>,
}

impl ApplyBlockRequest {
    pub fn convert_operations(block_operations: Vec<OperationsForBlocksMessage>) -> Vec<Vec<Operation>> {
        block_operations
            .into_iter()
            .map(|ops| ops.into())
            .collect()
    }
}

/// Application block result
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct ApplyBlockResponse {
    pub validation_result_message: String,
    pub context_hash: ContextHash,
    pub block_header_proto_json: String,
    pub block_header_proto_metadata_json: String,
    pub operations_proto_metadata_json: String,
    pub max_operations_ttl: i32,
    pub last_allowed_fork_level: i32,
    pub forking_testchain: bool,
    pub forking_testchain_data: Option<ForkingTestchainData>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct PrevalidatorWrapper {
    pub chain_id: ChainId,
    pub protocol: ProtocolHash,
    pub context_fitness: Option<Fitness>,
}

impl fmt::Debug for PrevalidatorWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PrevalidatorWrapper[chain_id: {}, protocol: {}, context_fitness: {}]",
               HashType::ChainId.bytes_to_string(&self.chain_id),
               HashType::ProtocolHash.bytes_to_string(&self.protocol),
               match &self.context_fitness {
                   Some(fitness) => display_fitness(fitness),
                   None => "-none-".to_string(),
               },
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Builder, PartialEq)]
pub struct BeginConstructionRequest {
    pub chain_id: ChainId,
    pub predecessor: BlockHeader,
    pub protocol_data: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Builder, PartialEq)]
pub struct ValidateOperationRequest {
    pub prevalidator: PrevalidatorWrapper,
    pub operation: Operation,
}

#[derive(Serialize, Deserialize, Debug, Clone, Builder, PartialEq)]
pub struct ValidateOperationResponse {
    pub prevalidator: PrevalidatorWrapper,
    pub result: ValidateOperationResult,
}

pub type OperationProtocolDataJson = String;
pub type ErrorListJson = String;

#[derive(Serialize, Deserialize, Clone, Builder, PartialEq)]
pub struct OperationProtocolDataJsonWithErrorListJson {
    pub protocol_data_json: OperationProtocolDataJson,
    pub error_json: ErrorListJson,
}

impl fmt::Debug for OperationProtocolDataJsonWithErrorListJson {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[error_json: {}, protocol_data_json: (-stripped-)]",
               &self.error_json,
        )
    }
}

trait HasOperationHash {
    fn operation_hash(&self) -> &OperationHash;
}

#[derive(Serialize, Deserialize, Clone, Builder, PartialEq)]
pub struct Applied {
    pub hash: OperationHash,
    pub protocol_data_json: OperationProtocolDataJson,
}

impl HasOperationHash for Applied {
    fn operation_hash(&self) -> &OperationHash {
        &self.hash
    }
}

impl fmt::Debug for Applied {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[hash: {}, protocol_data_json: {}]",
               HashType::OperationHash.bytes_to_string(&self.hash),
               &self.protocol_data_json
        )
    }
}

#[derive(Serialize, Deserialize, Clone, Builder, PartialEq)]
pub struct Errored {
    pub hash: OperationHash,
    pub is_endorsement: Option<bool>,
    pub protocol_data_json_with_error_json: OperationProtocolDataJsonWithErrorListJson,
}

impl HasOperationHash for Errored {
    fn operation_hash(&self) -> &OperationHash {
        &self.hash
    }
}

impl fmt::Debug for Errored {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[hash: {}, protocol_data_json_with_error_json: {:?}]",
               HashType::OperationHash.bytes_to_string(&self.hash),
               &self.protocol_data_json_with_error_json
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Builder, PartialEq, Default)]
pub struct ValidateOperationResult {
    pub applied: Vec<Applied>,
    pub refused: Vec<Errored>,
    pub branch_refused: Vec<Errored>,
    pub branch_delayed: Vec<Errored>,
    // TODO: outedate?
}

impl ValidateOperationResult {
    /// Merges result with new one, and returns `true/false` if something was changed
    pub fn merge(&mut self, new_result: ValidateOperationResult) -> bool {
        let mut changed = Self::merge_items(&mut self.applied, new_result.applied);
        changed |= Self::merge_items(&mut self.refused, new_result.refused);
        changed |= Self::merge_items(&mut self.branch_refused, new_result.branch_refused);
        changed |= Self::merge_items(&mut self.branch_delayed, new_result.branch_delayed);
        changed
    }

    fn merge_items<ITEM: HasOperationHash>(result_items: &mut Vec<ITEM>, new_items: Vec<ITEM>) -> bool {
        let mut changed = false;
        let mut added = false;

        for new_item in new_items {
            // check if present
            let old_value = result_items
                .iter()
                .position(|old_item| old_item.operation_hash().eq(new_item.operation_hash()));

            // replace or add
            if let Some(idx) = old_value {
                // replace
                result_items[idx] = new_item;
                changed |= true;
            } else {
                // add
                result_items.push(new_item);
                added |= true;
            }
        }

        added || changed
    }
}

/// Init protocol context result
#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct InitProtocolContextResult {
    pub supported_protocol_hashes: Vec<ProtocolHash>,
    /// Presents only if was genesis commited to context
    pub genesis_commit_hash: Option<ContextHash>,
}

impl fmt::Debug for InitProtocolContextResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let genesis_commit_hash = match &self.genesis_commit_hash {
            Some(hash) => HashType::ContextHash.bytes_to_string(hash),
            None => "-none-".to_string()
        };
        let supported_protocol_hashes = self.supported_protocol_hashes
            .iter()
            .map(|ph| HashType::ProtocolHash.bytes_to_string(ph))
            .collect::<Vec<String>>();
        write!(f, "genesis_commit_hash: {}, supported_protocol_hashes: {:?}", &genesis_commit_hash, &supported_protocol_hashes)
    }
}

/// Commit genesis result
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct CommitGenesisResult {
    pub block_header_proto_json: String,
    pub block_header_proto_metadata_json: String,
    pub operations_proto_metadata_json: String,
}

/// Forking test chain data
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct ForkingTestchainData {
    pub forking_block_hash: BlockHash,
    pub test_chain_id: ChainId,
}

#[derive(Serialize, Deserialize, Debug, Fail, PartialEq)]
pub enum CallError {
    #[fail(display = "Failed to call - message: {:?}!", parsed_error_message)]
    FailedToCall {
        parsed_error_message: Option<String>,
    },
    #[fail(display = "Invalid request data - message: {}!", message)]
    InvalidRequestData {
        message: String,
    },
    #[fail(display = "Invalid response data - message: {}!", message)]
    InvalidResponseData {
        message: String,
    },
}

impl From<OCamlError> for CallError {
    fn from(error: OCamlError) -> Self {
        match error {
            OCamlError::Exception(exception) => {
                match exception.message() {
                    None => CallError::FailedToCall {
                        parsed_error_message: None
                    },
                    Some(message) => {
                        CallError::FailedToCall {
                            parsed_error_message: Some(message)
                        }
                    }
                }
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Fail)]
pub enum TezosRuntimeConfigurationError {
    #[fail(display = "Change ocaml settings failed, message: {}!", message)]
    ChangeConfigurationError {
        message: String
    }
}

impl From<OCamlError> for TezosRuntimeConfigurationError {
    fn from(error: OCamlError) -> Self {
        match error {
            OCamlError::Exception(exception) => {
                TezosRuntimeConfigurationError::ChangeConfigurationError {
                    message: exception.message().unwrap_or_else(|| "unknown".to_string())
                }
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Fail)]
pub enum TezosStorageInitError {
    #[fail(display = "Ocaml storage init failed, message: {}!", message)]
    InitializeError {
        message: String
    }
}

impl From<OCamlError> for TezosStorageInitError {
    fn from(error: OCamlError) -> Self {
        match error {
            OCamlError::Exception(exception) => {
                TezosStorageInitError::InitializeError {
                    message: exception.message().unwrap_or_else(|| "unknown".to_string())
                }
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Fail)]
pub enum GetDataError {
    #[fail(display = "Ocaml failed to get data, message: {}!", message)]
    ReadError {
        message: String
    }
}

impl From<OCamlError> for GetDataError {
    fn from(error: OCamlError) -> Self {
        match error {
            OCamlError::Exception(exception) => {
                GetDataError::ReadError {
                    message: exception.message().unwrap_or_else(|| "unknown".to_string())
                }
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Fail, PartialEq)]
pub enum ApplyBlockError {
    #[fail(display = "Incomplete operations, exptected: {}, has actual: {}!", expected, actual)]
    IncompleteOperations {
        expected: usize,
        actual: usize,
    },
    #[fail(display = "Failed to apply block - message: {}!", message)]
    FailedToApplyBlock {
        message: String,
    },
    #[fail(display = "Unknown predecessor context - try to apply predecessor at first message: {}!", message)]
    UnknownPredecessorContext {
        message: String,
    },
    #[fail(display = "Predecessor does not match - message: {}!", message)]
    PredecessorMismatch {
        message: String,
    },
    #[fail(display = "Invalid request/response data - message: {}!", message)]
    InvalidRequestResponseData {
        message: String,
    },
}

impl From<CallError> for ApplyBlockError {
    fn from(error: CallError) -> Self {
        match error {
            CallError::FailedToCall { parsed_error_message } => {
                match parsed_error_message {
                    None => ApplyBlockError::FailedToApplyBlock {
                        message: "unknown".to_string()
                    },
                    Some(message) => {
                        match message.as_str() {
                            e if e.starts_with("Unknown_predecessor_context") => ApplyBlockError::UnknownPredecessorContext {
                                message: message.to_string()
                            },
                            e if e.starts_with("Predecessor_mismatch") => ApplyBlockError::PredecessorMismatch {
                                message: message.to_string()
                            },
                            message => ApplyBlockError::FailedToApplyBlock {
                                message: message.to_string()
                            }
                        }
                    }
                }
            }
            CallError::InvalidRequestData { message } => ApplyBlockError::InvalidRequestResponseData {
                message
            },
            CallError::InvalidResponseData { message } => ApplyBlockError::InvalidRequestResponseData {
                message
            },
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Fail, PartialEq)]
pub enum BeginConstructionError {
    #[fail(display = "Failed to begin construction - message: {}!", message)]
    FailedToBeginConstruction {
        message: String,
    },
    #[fail(display = "Unknown predecessor context - try to apply predecessor at first message: {}!", message)]
    UnknownPredecessorContext {
        message: String,
    },
    #[fail(display = "Invalid request/response data - message: {}!", message)]
    InvalidRequestResponseData {
        message: String,
    },
}

impl From<CallError> for BeginConstructionError {
    fn from(error: CallError) -> Self {
        match error {
            CallError::FailedToCall { parsed_error_message } => {
                match parsed_error_message {
                    None => BeginConstructionError::FailedToBeginConstruction {
                        message: "unknown".to_string()
                    },
                    Some(message) => {
                        match message.as_str() {
                            e if e.starts_with("Unknown_predecessor_context") => BeginConstructionError::UnknownPredecessorContext {
                                message: message.to_string()
                            },
                            message => BeginConstructionError::FailedToBeginConstruction {
                                message: message.to_string()
                            }
                        }
                    }
                }
            }
            CallError::InvalidRequestData { message } => BeginConstructionError::InvalidRequestResponseData {
                message
            },
            CallError::InvalidResponseData { message } => BeginConstructionError::InvalidRequestResponseData {
                message
            },
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Fail, PartialEq)]
pub enum ValidateOperationError {
    #[fail(display = "Failed to validate operation - message: {}!", message)]
    FailedToValidateOperation {
        message: String,
    },
    #[fail(display = "Invalid request/response data - message: {}!", message)]
    InvalidRequestResponseData {
        message: String,
    },
}

impl From<CallError> for ValidateOperationError {
    fn from(error: CallError) -> Self {
        match error {
            CallError::FailedToCall { parsed_error_message } => {
                match parsed_error_message {
                    None => ValidateOperationError::FailedToValidateOperation {
                        message: "unknown".to_string()
                    },
                    Some(message) => {
                        ValidateOperationError::FailedToValidateOperation {
                            message
                        }
                    }
                }
            }
            CallError::InvalidRequestData { message } => ValidateOperationError::InvalidRequestResponseData {
                message
            },
            CallError::InvalidResponseData { message } => ValidateOperationError::InvalidRequestResponseData {
                message
            },
        }
    }
}

#[derive(Debug, Fail)]
pub enum BlockHeaderError {
    #[fail(display = "BlockHeader cannot be read from storage: {}!", message)]
    ReadError {
        message: String
    },
    #[fail(display = "BlockHeader was expected, but was not found!")]
    ExpectedButNotFound,
}

impl From<OCamlError> for BlockHeaderError {
    fn from(error: OCamlError) -> Self {
        match error {
            OCamlError::Exception(exception) => {
                BlockHeaderError::ReadError {
                    message: exception.message().unwrap_or_else(|| "unknown".to_string())
                }
            }
        }
    }
}

#[derive(Debug, Fail)]
pub enum ContextDataError {
    #[fail(display = "Resolve/decode context data failed to decode: {}!", message)]
    DecodeError {
        message: String
    },
}

impl From<OCamlError> for ContextDataError {
    fn from(error: OCamlError) -> Self {
        match error {
            OCamlError::Exception(exception) => {
                ContextDataError::DecodeError {
                    message: exception.message().unwrap_or_else(|| "unknown".to_string())
                }
            }
        }
    }
}

pub type Json = String;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct JsonRpcRequest {
    pub body: Json,
    pub context_path: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct JsonRpcResponse {
    pub body: Json
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum RpcResponse {
    RPCConflict(Option<String>),
    RPCCreated(Option<String>),
    RPCError(Option<String>),
    RPCForbidden(Option<String>),
    RPCGone(Option<String>),
    RPCNotContent,
    RPCNotFound(Option<String>),
    RPCOk(String),
    RPCUnauthorized,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum RpcMethod {
    DELETE, GET, PATCH, POST, PUT
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct RpcArgDesc {
    name: String,
    descr: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum RpcError {
    RPCErrorCannotParseBody(String),
    RPCErrorCannotParsePath(Vec<String>, RpcArgDesc, String),
    RPCErrorCannotParseQuery(String),
    RPCErrorInvalidMethodString(String),
    RPCErrorMethodNotAllowed(Vec<RpcMethod>),
    RPCErrorNotFound,
}

#[derive(Serialize, Deserialize, Debug, Clone, Builder, PartialEq)]
pub struct ProtocolJsonRpcRequest {
    pub block_header: BlockHeader,
    pub chain_arg: String,
    pub chain_id: ChainId,

    pub request: JsonRpcRequest,

    // TODO: TE-140 - will be removed, when router is done
    pub ffi_service: FfiRpcService,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FfiRpcService {
    HelpersRunOperation,
    HelpersPreapplyOperations,
    HelpersPreapplyBlock,
    HelpersCurrentLevel,
    DelegatesMinimalValidTime,
    HelpersForgeOperations,
    ContextContract,
}

#[derive(Serialize, Deserialize, Debug, Fail, PartialEq)]
pub enum ProtocolRpcError {
    #[fail(display = "Failed to call protocol rpc - message: {}!", message)]
    FailedToCallProtocolRpc {
        message: String,
    },
    #[fail(display = "Invalid request data - message: {}!", message)]
    InvalidRequestData {
        message: String,
    },
    #[fail(display = "Invalid response data - message: {}!", message)]
    InvalidResponseData {
        message: String,
    },
}

impl From<CallError> for ProtocolRpcError {
    fn from(error: CallError) -> Self {
        match error {
            CallError::FailedToCall { parsed_error_message } => {
                match parsed_error_message {
                    None => ProtocolRpcError::FailedToCallProtocolRpc {
                        message: "unknown".to_string()
                    },
                    Some(message) => {
                        ProtocolRpcError::FailedToCallProtocolRpc {
                            message
                        }
                    }
                }
            }
            CallError::InvalidRequestData { message } => ProtocolRpcError::InvalidRequestData {
                message
            },
            CallError::InvalidResponseData { message } => ProtocolRpcError::InvalidResponseData {
                message
            },
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ComputePathRequest {
    pub operations: Vec<Vec<OperationHash>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ComputePathResponse {
    pub operations_hashes_path: Vec<Path>,
}

#[derive(Serialize, Deserialize, Debug, Fail)]
pub enum ComputePathError {
    #[fail(display = "Path computation failed, message: {}!", message)]
    PathError {
        message: String
    },
    #[fail(display = "Path computation failed, message: {}!", message)]
    InvalidRequestResponseData {
        message: String
    },
}

impl From<CallError> for ComputePathError {
    fn from(error: CallError) -> Self {
        match error {
            CallError::FailedToCall { parsed_error_message } => {
                match parsed_error_message {
                    None => ComputePathError::PathError {
                        message: "unknown".to_string()
                    },
                    Some(message) => {
                        ComputePathError::PathError {
                            message: message.to_string()
                        }
                    }
                }
            }
            CallError::InvalidRequestData { message } => ComputePathError::InvalidRequestResponseData {
                message
            },
            CallError::InvalidResponseData { message } => ComputePathError::InvalidRequestResponseData {
                message
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_operation_result_merge() -> Result<(), failure::Error> {
        let mut validate_result1 = validate_operation_result("onvN8U6QJ6DGJKVYkHXYRtFm3tgBJScj9P5bbPjSZUuFaGzwFuJ", "opVUxMhZttd858HXEHCgchknnnZFmUExtHrbmVSh1G9Pg24X1Pj");
        assert_eq!(2, validate_result1.applied.len());
        assert_eq!(2, validate_result1.refused.len());
        assert_eq!(2, validate_result1.branch_delayed.len());
        assert_eq!(2, validate_result1.branch_refused.len());

        // merge empty -> no change
        assert_eq!(
            false,
            validate_result1.merge(ValidateOperationResult {
                applied: vec![],
                refused: vec![],
                branch_refused: vec![],
                branch_delayed: vec![],
            })
        );
        assert_eq!(2, validate_result1.applied.len());
        assert_eq!(2, validate_result1.refused.len());
        assert_eq!(2, validate_result1.branch_delayed.len());
        assert_eq!(2, validate_result1.branch_refused.len());

        // merge
        let validate_result2 = validate_operation_result("onvN8U6QJ6DGJKVYkHXYRtFm3tgBJScj9P5bbPjSZUuFaGzwFuJ", "opJ4FdKumPfykAP9ZqwY7rNB8y1SiMupt44RqBDMWL7cmb4xbNr");
        assert!(validate_result1.merge(validate_result2));
        assert_eq!(3, validate_result1.applied.len());
        assert_eq!(3, validate_result1.refused.len());
        assert_eq!(3, validate_result1.branch_delayed.len());
        assert_eq!(3, validate_result1.branch_refused.len());

        Ok(())
    }

    #[test]
    fn test_validate_operation_result_merge_items() -> Result<(), failure::Error> {
        let mut validate_result = ValidateOperationResult {
            applied: vec![],
            refused: vec![],
            branch_refused: vec![],
            branch_delayed: vec![],
        };
        assert_eq!(0, validate_result.applied.len());
        assert_eq!(false, ValidateOperationResult::merge_items(&mut validate_result.applied, vec![]));

        assert!(ValidateOperationResult::merge_items(&mut validate_result.applied, vec![
            Applied {
                hash: HashType::OperationHash.string_to_bytes("onvN8U6QJ6DGJKVYkHXYRtFm3tgBJScj9P5bbPjSZUuFaGzwFuJ")?,
                protocol_data_json: "protocol_data_json1".to_string(),
            },
        ]));
        assert_eq!(1, validate_result.applied.len());
        assert_eq!("protocol_data_json1", validate_result.applied[0].protocol_data_json);

        // merge the same -> test change
        assert!(ValidateOperationResult::merge_items(&mut validate_result.applied, vec![
            Applied {
                hash: HashType::OperationHash.string_to_bytes("onvN8U6QJ6DGJKVYkHXYRtFm3tgBJScj9P5bbPjSZUuFaGzwFuJ")?,
                protocol_data_json: "protocol_data_json2".to_string(),
            },
        ]));
        assert_eq!(1, validate_result.applied.len());
        assert_eq!("protocol_data_json2", validate_result.applied[0].protocol_data_json);

        // merge another new one
        assert!(ValidateOperationResult::merge_items(&mut validate_result.applied, vec![
            Applied {
                hash: HashType::OperationHash.string_to_bytes("opJ4FdKumPfykAP9ZqwY7rNB8y1SiMupt44RqBDMWL7cmb4xbNr")?,
                protocol_data_json: "protocol_data_json2".to_string(),
            },
        ]));
        assert_eq!(2, validate_result.applied.len());

        Ok(())
    }

    fn validate_operation_result(op1: &str, op2: &str) -> ValidateOperationResult {
        let applied = vec![
            Applied {
                hash: HashType::OperationHash.string_to_bytes(op1).expect("Error"),
                protocol_data_json: "{ \"contents\": [ { \"kind\": \"endorsement\", \"level\": 459020 } ],\n  \"signature\":\n    \"siguKbKFVDkXo2m1DqZyftSGg7GZRq43EVLSutfX5yRLXXfWYG5fegXsDT6EUUqawYpjYE1GkyCVHfc2kr3hcaDAvWSAhnV9\" }".to_string(),
            },
            Applied {
                hash: HashType::OperationHash.string_to_bytes(op2).expect("Error"),
                protocol_data_json: "{ \"contents\": [ { \"kind\": \"endorsement\", \"level\": 459020 } ],\n  \"signature\":\n    \"siguKbKFVDkXo2m1DqZyftSGg7GZRq43EVLSutfX5yRLXXfWYG5fegXsDT6EUUqawYpjYE1GkyCVHfc2kr3hcaDAvWSAhnV9\" }".to_string(),
            }
        ];

        let branch_delayed = vec![
            Errored {
                hash: HashType::OperationHash.string_to_bytes(op1).expect("Error"),
                is_endorsement: None,
                protocol_data_json_with_error_json: OperationProtocolDataJsonWithErrorListJson {
                    protocol_data_json: "{ \"contents\": [ { \"kind\": \"endorsement\", \"level\": 459020 } ],\n  \"signature\":\n    \"siguKbKFVDkXo2m1DqZyftSGg7GZRq43EVLSutfX5yRLXXfWYG5fegXsDT6EUUqawYpjYE1GkyCVHfc2kr3hcaDAvWSAhnV9\" }".to_string(),
                    error_json: "[ { \"kind\": \"temporary\",\n    \"id\": \"proto.005-PsBabyM1.operation.wrong_endorsement_predecessor\",\n    \"expected\": \"BMDb9PfcJmiibDDEbd6bEEDj4XNG4C7QACG6TWqz29c9FxNgDLL\",\n    \"provided\": \"BLd8dLs4X5Ve6a8B37kUu7iJkRycWzfSF5MrskY4z8YaideQAp4\" } ]".to_string(),
                },
            },
            Errored {
                hash: HashType::OperationHash.string_to_bytes(op2).expect("Error"),
                is_endorsement: None,
                protocol_data_json_with_error_json: OperationProtocolDataJsonWithErrorListJson {
                    protocol_data_json: "{ \"contents\": [ { \"kind\": \"endorsement\", \"level\": 459020 } ],\n  \"signature\":\n    \"siguKbKFVDkXo2m1DqZyftSGg7GZRq43EVLSutfX5yRLXXfWYG5fegXsDT6EUUqawYpjYE1GkyCVHfc2kr3hcaDAvWSAhnV9\" }".to_string(),
                    error_json: "[ { \"kind\": \"temporary\",\n    \"id\": \"proto.005-PsBabyM1.operation.wrong_endorsement_predecessor\",\n    \"expected\": \"BMDb9PfcJmiibDDEbd6bEEDj4XNG4C7QACG6TWqz29c9FxNgDLL\",\n    \"provided\": \"BLd8dLs4X5Ve6a8B37kUu7iJkRycWzfSF5MrskY4z8YaideQAp4\" } ]".to_string(),
                },
            }
        ];

        let branch_refused = vec![
            Errored {
                hash: HashType::OperationHash.string_to_bytes(op1).expect("Error"),
                is_endorsement: None,
                protocol_data_json_with_error_json: OperationProtocolDataJsonWithErrorListJson {
                    protocol_data_json: "{ \"contents\": [ { \"kind\": \"endorsement\", \"level\": 459020 } ],\n  \"signature\":\n    \"siguKbKFVDkXo2m1DqZyftSGg7GZRq43EVLSutfX5yRLXXfWYG5fegXsDT6EUUqawYpjYE1GkyCVHfc2kr3hcaDAvWSAhnV9\" }".to_string(),
                    error_json: "[ { \"kind\": \"temporary\",\n    \"id\": \"proto.005-PsBabyM1.operation.wrong_endorsement_predecessor\",\n    \"expected\": \"BMDb9PfcJmiibDDEbd6bEEDj4XNG4C7QACG6TWqz29c9FxNgDLL\",\n    \"provided\": \"BLd8dLs4X5Ve6a8B37kUu7iJkRycWzfSF5MrskY4z8YaideQAp4\" } ]".to_string(),
                },
            },
            Errored {
                hash: HashType::OperationHash.string_to_bytes(op2).expect("Error"),
                is_endorsement: None,
                protocol_data_json_with_error_json: OperationProtocolDataJsonWithErrorListJson {
                    protocol_data_json: "{ \"contents\": [ { \"kind\": \"endorsement\", \"level\": 459020 } ],\n  \"signature\":\n    \"siguKbKFVDkXo2m1DqZyftSGg7GZRq43EVLSutfX5yRLXXfWYG5fegXsDT6EUUqawYpjYE1GkyCVHfc2kr3hcaDAvWSAhnV9\" }".to_string(),
                    error_json: "[ { \"kind\": \"temporary\",\n    \"id\": \"proto.005-PsBabyM1.operation.wrong_endorsement_predecessor\",\n    \"expected\": \"BMDb9PfcJmiibDDEbd6bEEDj4XNG4C7QACG6TWqz29c9FxNgDLL\",\n    \"provided\": \"BLd8dLs4X5Ve6a8B37kUu7iJkRycWzfSF5MrskY4z8YaideQAp4\" } ]".to_string(),
                },
            }
        ];

        let refused = vec![
            Errored {
                hash: HashType::OperationHash.string_to_bytes(op1).expect("Error"),
                is_endorsement: None,
                protocol_data_json_with_error_json: OperationProtocolDataJsonWithErrorListJson {
                    protocol_data_json: "{ \"contents\": [ { \"kind\": \"endorsement\", \"level\": 459020 } ],\n  \"signature\":\n    \"siguKbKFVDkXo2m1DqZyftSGg7GZRq43EVLSutfX5yRLXXfWYG5fegXsDT6EUUqawYpjYE1GkyCVHfc2kr3hcaDAvWSAhnV9\" }".to_string(),
                    error_json: "[ { \"kind\": \"temporary\",\n    \"id\": \"proto.005-PsBabyM1.operation.wrong_endorsement_predecessor\",\n    \"expected\": \"BMDb9PfcJmiibDDEbd6bEEDj4XNG4C7QACG6TWqz29c9FxNgDLL\",\n    \"provided\": \"BLd8dLs4X5Ve6a8B37kUu7iJkRycWzfSF5MrskY4z8YaideQAp4\" } ]".to_string(),
                },
            },
            Errored {
                hash: HashType::OperationHash.string_to_bytes(op2).expect("Error"),
                is_endorsement: None,
                protocol_data_json_with_error_json: OperationProtocolDataJsonWithErrorListJson {
                    protocol_data_json: "{ \"contents\": [ { \"kind\": \"endorsement\", \"level\": 459020 } ],\n  \"signature\":\n    \"siguKbKFVDkXo2m1DqZyftSGg7GZRq43EVLSutfX5yRLXXfWYG5fegXsDT6EUUqawYpjYE1GkyCVHfc2kr3hcaDAvWSAhnV9\" }".to_string(),
                    error_json: "[ { \"kind\": \"temporary\",\n    \"id\": \"proto.005-PsBabyM1.operation.wrong_endorsement_predecessor\",\n    \"expected\": \"BMDb9PfcJmiibDDEbd6bEEDj4XNG4C7QACG6TWqz29c9FxNgDLL\",\n    \"provided\": \"BLd8dLs4X5Ve6a8B37kUu7iJkRycWzfSF5MrskY4z8YaideQAp4\" } ]".to_string(),
                },
            }
        ];

        ValidateOperationResult {
            applied,
            branch_delayed,
            branch_refused,
            refused,
        }
    }
}