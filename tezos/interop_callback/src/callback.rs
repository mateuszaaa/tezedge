// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! This module provides all the FFI callback functions.

use ocaml_interop::{ocaml_export, ToRust, OCaml, OCamlInt64, OCamlList, RawOCaml};

use tezos_context::channel::*;

type Hash = Vec<u8>;
type ContextHash = Hash;
type BlockHash = Hash;
type OperationHash = Hash;
type ContextKey = Vec<String>;
type ContextValue = Vec<u8>;

extern "C" {
    fn initialize_ml_context_functions(
        ml_context_set: unsafe extern "C" fn(RawOCaml, RawOCaml, RawOCaml, RawOCaml, f64, f64) -> RawOCaml,
        ml_context_delete: unsafe extern "C" fn(RawOCaml, RawOCaml, RawOCaml, RawOCaml, f64, f64) -> RawOCaml,
        ml_context_remove_rec: unsafe extern "C" fn(RawOCaml, RawOCaml, RawOCaml, RawOCaml, f64, f64) -> RawOCaml,
        ml_context_copy: unsafe extern "C" fn(RawOCaml, RawOCaml, RawOCaml, RawOCaml, f64, f64) -> RawOCaml,
        ml_context_checkout: unsafe extern "C" fn(RawOCaml, f64, f64) -> RawOCaml,
        ml_context_commit: unsafe extern "C" fn(RawOCaml, RawOCaml, RawOCaml, RawOCaml, f64, f64) -> RawOCaml,
        ml_context_mem: unsafe extern "C" fn(RawOCaml, RawOCaml, RawOCaml, RawOCaml, f64, f64) -> RawOCaml,
        ml_context_dir_mem: unsafe extern "C" fn(RawOCaml, RawOCaml, RawOCaml, RawOCaml, f64, f64) -> RawOCaml,
        ml_context_raw_get: unsafe extern "C" fn(RawOCaml, RawOCaml, RawOCaml, RawOCaml, f64, f64) -> RawOCaml,
        ml_context_fold: unsafe extern "C" fn(RawOCaml, RawOCaml, RawOCaml, RawOCaml, f64, f64) -> RawOCaml,
    );
}

pub fn initialize_callbacks() {
    unsafe {
        initialize_ml_context_functions(
            real_ml_context_set,
            real_ml_context_delete,
            real_ml_context_remove_rec,
            real_ml_context_copy,
            real_ml_context_checkout,
            real_ml_context_commit,
            real_ml_context_mem,
            real_ml_context_dir_mem,
            real_ml_context_raw_get,
            real_ml_context_fold)
    }
}

ocaml_export! {

    // External callback function for set value to context
    fn real_ml_context_set(gc,
        context_hash: OCaml<Option<String>>,
        block_hash: OCaml<Option<String>>,
        operation_hash: OCaml<Option<String>>,
        keyval_and_json: OCaml<(OCamlList<String>, String, Option<String>, bool)>,
        start_time: f64,
        end_time: f64,
    ) {
        let context_hash = context_hash.to_rust();
        let block_hash = block_hash.to_rust();
        let operation_hash = operation_hash.to_rust();
        let (key, value, json_val, ignored) = keyval_and_json.to_rust();

        context_set(context_hash, block_hash, operation_hash, key, value, json_val, ignored, start_time, end_time);
        OCaml::unit()
    }

    // External callback function for delete key from context
    fn real_ml_context_delete(gc,
        context_hash: OCaml<Option<String>>,
        block_hash: OCaml<Option<String>>,
        operation_hash: OCaml<Option<String>>,
        keyval: OCaml<(OCamlList<String>, bool)>,
        start_time: f64,
        end_time: f64,
    ) {
        let context_hash = context_hash.to_rust();
        let block_hash = block_hash.to_rust();
        let operation_hash = operation_hash.to_rust();
        let (key, ignored) = keyval.to_rust();

        context_delete(context_hash, block_hash, operation_hash, key, ignored, start_time, end_time);
        OCaml::unit()
    }

    // External callback function for remove_rec key from context
    fn real_ml_context_remove_rec(gc,
        context_hash: OCaml<Option<String>>,
        block_hash: OCaml<Option<String>>,
        operation_hash: OCaml<Option<String>>,
        keyval: OCaml<(OCamlList<String>, bool)>,
        start_time: f64,
        end_time: f64,
    ) {
        let context_hash = context_hash.to_rust();
        let block_hash = block_hash.to_rust();
        let operation_hash = operation_hash.to_rust();
        let (key, ignored) = keyval.to_rust();

        context_remove_rec(context_hash, block_hash, operation_hash, key, ignored, start_time, end_time);
        OCaml::unit()
    }

    // External callback function for copy keys from context
    fn real_ml_context_copy(gc,
        context_hash: OCaml<Option<String>>,
        block_hash: OCaml<Option<String>>,
        operation_hash: OCaml<Option<String>>,
        from_to_key: OCaml<(OCamlList<String>, OCamlList<String>, bool)>,
        start_time: f64,
        end_time: f64,
    ) {
        let context_hash = context_hash.to_rust();
        let block_hash = block_hash.to_rust();
        let operation_hash = operation_hash.to_rust();
        let (from_key, to_key, ignored) = from_to_key.to_rust();

        context_copy(context_hash, block_hash, operation_hash, from_key, to_key, ignored, start_time, end_time);
        OCaml::unit()
    }

    // External callback function for checkout context
    fn real_ml_context_checkout(gc,
        context_hash: OCaml<String>,
        start_time: f64,
        end_time: f64,
    ) {
        let context_hash = context_hash.to_rust();

        context_checkout(context_hash, start_time, end_time);
        OCaml::unit()
    }

    // External callback function for checkout context
    fn real_ml_context_commit(gc,
        parent_context_hash: OCaml<Option<String>>,
        block_hash: OCaml<Option<String>>,
        new_context_hash: OCaml<String>,
        info: OCaml<(OCamlInt64, String, String, OCamlList<String>)>,
        start_time: f64,
        end_time: f64,
    ) {
        let parent_context_hash = parent_context_hash.to_rust();
        let block_hash = block_hash.to_rust();
        let new_context_hash = new_context_hash.to_rust();

        let (date, author, message, parents) = info.to_rust();

        context_commit(parent_context_hash, block_hash, new_context_hash, date, author, message, parents, start_time, end_time);
        OCaml::unit()
    }

    // External callback function for mem key from context
    fn real_ml_context_mem(gc,
        context_hash: OCaml<Option<String>>,
        block_hash: OCaml<Option<String>>,
        operation_hash: OCaml<Option<String>>,
        keyval: OCaml<(OCamlList<String>, bool)>,
        start_time: f64,
        end_time: f64,
    ) {
        let context_hash = context_hash.to_rust();
        let block_hash = block_hash.to_rust();
        let operation_hash = operation_hash.to_rust();
        let (key, value) = keyval.to_rust();

        context_mem(context_hash, block_hash, operation_hash, key, value, start_time, end_time);
        OCaml::unit()
    }

    // External callback function for dir_mem key from context
    fn real_ml_context_dir_mem(gc,
        context_hash: OCaml<Option<String>>,
        block_hash: OCaml<Option<String>>,
        operation_hash: OCaml<Option<String>>,
        keyval: OCaml<(OCamlList<String>, bool)>,
        start_time: f64,
        end_time: f64,
    ) {
        let context_hash = context_hash.to_rust();
        let block_hash = block_hash.to_rust();
        let operation_hash = operation_hash.to_rust();
        let (key, value) = keyval.to_rust();

        context_dir_mem(context_hash, block_hash, operation_hash, key, value, start_time, end_time);
        OCaml::unit()
    }

    // External callback function for raw_get key from context
    fn real_ml_context_raw_get(gc,
        context_hash: OCaml<Option<String>>,
        block_hash: OCaml<Option<String>>,
        operation_hash: OCaml<Option<String>>,
        keyval_and_json: OCaml<(OCamlList<String>, String, Option<String>)>,
        start_time: f64,
        end_time: f64,
    ) {
        let context_hash = context_hash.to_rust();
        let block_hash = block_hash.to_rust();
        let operation_hash = operation_hash.to_rust();
        let (key, value, json_val) =  keyval_and_json.to_rust();

        context_raw_get(context_hash, block_hash, operation_hash, key, value, json_val, start_time, end_time);
        OCaml::unit()
    }

    // External callback function for fold key from context
    fn real_ml_context_fold(gc,
        context_hash: OCaml<Option<String>>,
        block_hash: OCaml<Option<String>>,
        operation_hash: OCaml<Option<String>>,
        key: OCaml<OCamlList<String>>,
        start_time: f64,
        end_time: f64,
    ) {
        let context_hash = context_hash.to_rust();
        let block_hash = block_hash.to_rust();
        let operation_hash = operation_hash.to_rust();
        let key = key.to_rust();

        context_fold(context_hash, block_hash, operation_hash, key, start_time, end_time);
        OCaml::unit()
    }
}

fn context_set(
    context_hash: Option<ContextHash>,
    block_hash: Option<BlockHash>,
    operation_hash: Option<OperationHash>,
    key: ContextKey,
    value: ContextValue,
    value_as_json: Option<String>,
    ignored: bool,
    start_time: f64,
    end_time: f64,
) {
    context_send(ContextAction::Set {
        context_hash,
        block_hash,
        operation_hash,
        key,
        value,
        value_as_json,
        ignored,
        start_time,
        end_time,
    })
    .expect("context_set error");
}

fn context_delete(
    context_hash: Option<ContextHash>,
    block_hash: Option<BlockHash>,
    operation_hash: Option<OperationHash>,
    key: ContextKey,
    ignored: bool,
    start_time: f64,
    end_time: f64,
) {
    context_send(ContextAction::Delete {
        context_hash,
        block_hash,
        operation_hash,
        key,
        ignored,
        start_time,
        end_time,
    })
    .expect("context_delete error");
}

fn context_remove_rec(
    context_hash: Option<ContextHash>,
    block_hash: Option<BlockHash>,
    operation_hash: Option<OperationHash>,
    key: ContextKey,
    ignored: bool,
    start_time: f64,
    end_time: f64,
) {
    context_send(ContextAction::RemoveRecursively {
        context_hash,
        block_hash,
        operation_hash,
        key,
        ignored,
        start_time,
        end_time,
    })
    .expect("context_remove_rec error");
}

fn context_copy(
    context_hash: Option<ContextHash>,
    block_hash: Option<BlockHash>,
    operation_hash: Option<OperationHash>,
    from_key: ContextKey,
    to_key: ContextKey,
    ignored: bool,
    start_time: f64,
    end_time: f64,
) {
    context_send(ContextAction::Copy {
        context_hash,
        block_hash,
        operation_hash,
        from_key,
        to_key,
        ignored,
        start_time,
        end_time,
    })
    .expect("context_copy error");
}

fn context_checkout(context_hash: ContextHash, start_time: f64, end_time: f64) {
    context_send(ContextAction::Checkout {
        context_hash,
        start_time,
        end_time,
    })
    .expect("context_checkout error");
}

fn context_commit(
    parent_context_hash: Option<ContextHash>,
    block_hash: Option<BlockHash>,
    new_context_hash: ContextHash,
    date: i64,
    author: String,
    message: String,
    parents: Vec<Vec<u8>>,
    start_time: f64,
    end_time: f64)
{
    context_send(ContextAction::Commit {
        parent_context_hash,
        block_hash,
        new_context_hash,
        author,
        message,
        date,
        parents,
        start_time,
        end_time,
    }).expect("context_commit error");
}

fn context_mem(
    context_hash: Option<ContextHash>,
    block_hash: Option<BlockHash>,
    operation_hash: Option<OperationHash>,
    key: ContextKey,
    value: bool,
    start_time: f64,
    end_time: f64,
) {
    context_send(ContextAction::Mem {
        context_hash,
        block_hash,
        operation_hash,
        key,
        value,
        start_time,
        end_time,
    })
    .expect("context_mem error");
}

fn context_dir_mem(
    context_hash: Option<ContextHash>,
    block_hash: Option<BlockHash>,
    operation_hash: Option<OperationHash>,
    key: ContextKey,
    value: bool,
    start_time: f64,
    end_time: f64,
) {
    context_send(ContextAction::DirMem {
        context_hash,
        block_hash,
        operation_hash,
        key,
        value,
        start_time,
        end_time,
    })
    .expect("context_dir_mem error");
}

fn context_raw_get(
    context_hash: Option<ContextHash>,
    block_hash: Option<BlockHash>,
    operation_hash: Option<OperationHash>,
    key: ContextKey,
    value: ContextValue,
    value_as_json: Option<String>,
    start_time: f64,
    end_time: f64,
) {
    context_send(ContextAction::Get {
        context_hash,
        block_hash,
        operation_hash,
        key,
        value,
        value_as_json,
        start_time,
        end_time,
    })
    .expect("context_get error");
}

fn context_fold(
    context_hash: Option<ContextHash>,
    block_hash: Option<BlockHash>,
    operation_hash: Option<OperationHash>,
    key: ContextKey,
    start_time: f64,
    end_time: f64,
) {
    context_send(ContextAction::Fold {
        context_hash,
        block_hash,
        operation_hash,
        key,
        start_time,
        end_time,
    })
    .expect("context_fold error");
}
