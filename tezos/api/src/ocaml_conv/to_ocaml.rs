// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use super::{
    FfiBlockHeader, FfiBlockHeaderShellHeader, FfiOperation, FfiOperationShellHeader,
    OCamlBlockHash, OCamlContextHash, OCamlHash, OCamlOperationHash, OCamlOperationListListHash,
    OCamlProtocolHash, TaggedHash,
};
use crate::ffi::{
    ApplyBlockRequest, ApplyBlockResponse, BeginConstructionRequest,
    ForkingTestchainData, JsonRpcRequest, PrevalidatorWrapper, ProtocolJsonRpcRequest,
    ValidateOperationRequest,
};
use crypto::hash::{BlockHash, ContextHash, Hash, OperationListListHash, ProtocolHash};
use tezos_messages::p2p::encoding::prelude::{BlockHeader, Operation};
use ocaml_interop::{
    impl_to_ocaml_record, ocaml_alloc_record, ocaml_alloc_variant,
    OCamlAllocResult, OCamlAllocToken, OCamlBytes, OCamlInt, OCamlInt32, OCamlInt64, OCamlList,
    ToOCaml,
};

// OCaml type tags

struct BlockHeaderShellHeader {}
struct OperationShellHeader {}

// Hashes

impl<'a> From<&'a Hash> for TaggedHash<'a> {
    fn from(bytes: &'a Hash) -> Self {
        TaggedHash::Hash(bytes)
    }
}

unsafe impl<'a> ToOCaml<OCamlHash> for TaggedHash<'a> {
    fn to_ocaml(&self, _token: OCamlAllocToken) -> OCamlAllocResult<OCamlHash> {
        ocaml_alloc_variant! {
            self => {
                TaggedHash::Hash(hash: OCamlBytes)
            }
        }
    }
}

macro_rules! to_ocaml_hash {
    ($ocaml_name:ty, $rust_name:ty) => {
        unsafe impl ToOCaml<$ocaml_name> for $rust_name {
            fn to_ocaml(&self, _token: OCamlAllocToken) -> OCamlAllocResult<$ocaml_name> {
                let tagged = TaggedHash::Hash(self);
                ocaml_alloc_variant! {
                    tagged => {
                        TaggedHash::Hash(hash: OCamlBytes)
                    }
                }
            }
        }
    };
}

to_ocaml_hash!(OCamlOperationListListHash, OperationListListHash);
to_ocaml_hash!(OCamlOperationHash, Hash);
to_ocaml_hash!(OCamlBlockHash, BlockHash);
to_ocaml_hash!(OCamlContextHash, ContextHash);
to_ocaml_hash!(OCamlProtocolHash, ProtocolHash);

// Other

impl<'a> From<&'a BlockHeader> for FfiBlockHeaderShellHeader<'a> {
    fn from(block_header: &'a BlockHeader) -> Self {
        Self {
            level: block_header.level(),
            proto_level: block_header.proto() as i32,
            predecessor: block_header.predecessor().into(),
            timestamp: block_header.timestamp(),
            validation_passes: block_header.validation_pass() as i32,
            operations_hash: block_header.operations_hash().into(),
            fitness: block_header.fitness(),
            context: block_header.context().into(),
        }
    }
}

impl<'a> From<&'a BlockHeader> for FfiBlockHeader<'a> {
    fn from(block_header: &'a BlockHeader) -> Self {
        let shell = FfiBlockHeaderShellHeader::from(block_header);
        Self {
            shell,
            protocol_data: block_header.protocol_data(),
        }
    }
}

impl<'a> From<&'a Operation> for FfiOperationShellHeader<'a> {
    fn from(operation: &'a Operation) -> Self {
        Self {
            branch: TaggedHash::Hash(operation.branch()),
        }
    }
}

impl<'a> From<&'a Operation> for FfiOperation<'a> {
    fn from(operation: &'a Operation) -> Self {
        let shell = FfiOperationShellHeader::from(operation);
        Self {
            shell,
            data: operation.data(),
        }
    }
}

impl_to_ocaml_record! {
    ApplyBlockRequest {
        chain_id: OCamlBytes,
        block_header: BlockHeader => FfiBlockHeader::from(block_header),
        pred_header: BlockHeader => FfiBlockHeader::from(pred_header),
        max_operations_ttl: OCamlInt,
        operations: OCamlList<OCamlList<Operation>> => {
            operations.iter()
                      .map(|ops| ops.iter().map(|op| FfiOperation::from(op)).collect())
                      .collect::<Vec<Vec<FfiOperation>>>()
        },
    }
}

impl_to_ocaml_record! {
    ApplyBlockResponse {
        validation_result_message: OCamlBytes,
        context_hash: OCamlContextHash,
        block_header_proto_json: OCamlBytes,
        block_header_proto_metadata_json: OCamlBytes,
        operations_proto_metadata_json: OCamlBytes,
        max_operations_ttl: OCamlInt,
        last_allowed_fork_level: OCamlInt32,
        forking_testchain: bool,
        forking_testchain_data: Option<ForkingTestchainData>,
    }
}

impl_to_ocaml_record! {
    ForkingTestchainData {
        forking_block_hash: OCamlBlockHash,
        test_chain_id: OCamlBytes,
    }
}

impl_to_ocaml_record! {
    BeginConstructionRequest {
        chain_id: OCamlBytes,
        predecessor: BlockHeader => FfiBlockHeader::from(predecessor),
        protocol_data: Option<OCamlBytes>,
    }
}

impl_to_ocaml_record! {
    PrevalidatorWrapper {
        chain_id: OCamlBytes,
        protocol: OCamlProtocolHash,
        context_fitness: Option<OCamlList<OCamlBytes>>
    }
}

impl_to_ocaml_record! {
    ValidateOperationRequest {
        prevalidator: PrevalidatorWrapper,
        operation: Operation => FfiOperation::from(operation),
    }
}

impl_to_ocaml_record! {
    JsonRpcRequest {
        body: OCamlBytes,
        context_path: OCamlBytes,
        meth: OCamlBytes,
        content_type: Option<OCamlBytes>,
    }
}

impl_to_ocaml_record! {
    ProtocolJsonRpcRequest {
        block_header: BlockHeader => FfiBlockHeader::from(block_header),
        chain_id: OCamlBytes,
        chain_arg: OCamlBytes,
        request: JsonRpcRequest,
    }
}

unsafe impl<'a> ToOCaml<BlockHeaderShellHeader> for FfiBlockHeaderShellHeader<'a> {
    fn to_ocaml(&self, _token: OCamlAllocToken) -> OCamlAllocResult<BlockHeaderShellHeader> {
        ocaml_alloc_record! {
            self {
                level: OCamlInt32,
                proto_level: OCamlInt,
                predecessor: OCamlHash,
                timestamp: OCamlInt64,
                validation_passes: OCamlInt,
                operations_hash: OCamlHash,
                fitness: OCamlList<OCamlBytes>,
                context: OCamlHash,
            }
        }
    }
}

unsafe impl<'a> ToOCaml<BlockHeader> for FfiBlockHeader<'a> {
    fn to_ocaml(&self, _token: OCamlAllocToken) -> OCamlAllocResult<BlockHeader> {
        ocaml_alloc_record! {
            self {
                shell: BlockHeaderShellHeader,
                protocol_data: OCamlBytes,
            }
        }
    }
}

unsafe impl<'a> ToOCaml<OperationShellHeader> for FfiOperationShellHeader<'a> {
    fn to_ocaml(&self, _token: OCamlAllocToken) -> OCamlAllocResult<OperationShellHeader> {
        ocaml_alloc_record! {
            self {
                branch: OCamlHash,
            }
        }
    }
}

unsafe impl<'a> ToOCaml<Operation> for FfiOperation<'a> {
    fn to_ocaml(&self, _token: OCamlAllocToken) -> OCamlAllocResult<Operation> {
        ocaml_alloc_record! {
            self {
                shell: OperationShellHeader,
                data: OCamlBytes,
            }
        }
    }
}
