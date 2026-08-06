package synap

import (
	"context"
	"fmt"
)

// TransactionManager opens Redis-compatible MULTI/EXEC transactions.
//
// A transaction is keyed server-side by a client id, so every command that
// belongs to it must carry the same id. Rather than threading that id through
// every call site, [TransactionManager.Begin] returns a [Tx] whose module
// accessors issue their commands inside the transaction:
//
//	tx, err := client.Transaction().Multi(ctx)
//	if err != nil { return err }
//	if err := tx.KV().Set(ctx, "a", "1", 0); err != nil {
//	    _ = tx.Discard(ctx)
//	    return err
//	}
//	result, err := tx.Exec(ctx)
//
// Writes issued through a Tx are queued by the server (they travel as TXQUEUE
// on the native transports) and applied atomically by Exec. A command the
// server cannot queue is refused rather than silently executed outside the
// transaction.
type TransactionManager struct {
	client *SynapClient
}

// Transaction returns the transaction manager for this client.
func (c *SynapClient) Transaction() *TransactionManager { return &TransactionManager{client: c} }

// Tx is an open transaction scope. Commands issued through its module
// accessors carry the transaction's client id.
type Tx struct {
	client   *SynapClient
	clientID string
}

// ExecResult reports the outcome of [Tx.Exec].
type ExecResult struct {
	// Success is false when the transaction aborted.
	Success bool `json:"success"`
	// Aborted is true when a watched key changed before Exec, so the queued
	// commands were discarded and nothing was applied.
	Aborted bool `json:"aborted"`
	// Results holds one entry per queued command, in queue order.
	Results []interface{} `json:"results"`
}

// Begin opens a transaction scope without contacting the server. Use it when
// you need to WATCH keys before issuing MULTI; otherwise call
// [TransactionManager.Multi], which does both.
func (t *TransactionManager) Begin() (*Tx, error) {
	clientID, err := newRequestID()
	if err != nil {
		return nil, fmt.Errorf("synap: generate transaction client_id: %w", err)
	}
	return t.scope(clientID), nil
}

// Multi opens a transaction scope and issues MULTI on it.
func (t *TransactionManager) Multi(ctx context.Context) (*Tx, error) {
	tx, err := t.Begin()
	if err != nil {
		return nil, err
	}
	if err := tx.Multi(ctx); err != nil {
		return nil, err
	}
	return tx, nil
}

// Resume rebinds an existing transaction by its client id — for a transaction
// opened by another process, or one whose id you persisted.
func (t *TransactionManager) Resume(clientID string) *Tx {
	return t.scope(clientID)
}

// scope returns a Tx backed by a shallow copy of the client carrying clientID,
// so the caller's own client keeps issuing non-transactional commands.
func (t *TransactionManager) scope(clientID string) *Tx {
	scoped := *t.client
	scoped.txClientID = clientID
	return &Tx{client: &scoped, clientID: clientID}
}

// ClientID returns the id that keys this transaction server-side.
func (t *Tx) ClientID() string { return t.clientID }

// KV returns a KV store scoped to this transaction.
func (t *Tx) KV() *KVStore { return &KVStore{client: t.client} }

// Hash returns a hash manager scoped to this transaction.
func (t *Tx) Hash() *HashManager { return &HashManager{client: t.client} }

// List returns a list manager scoped to this transaction.
func (t *Tx) List() *ListManager { return &ListManager{client: t.client} }

// Set returns a set manager scoped to this transaction.
func (t *Tx) Set() *SetManager { return &SetManager{client: t.client} }

// Multi starts the transaction. Commands issued through this Tx afterwards are
// queued rather than applied, until Exec or Discard.
func (t *Tx) Multi(ctx context.Context) error {
	_, err := t.control(ctx, "transaction.multi", nil)
	return err
}

// Discard drops the queued commands and closes the transaction.
func (t *Tx) Discard(ctx context.Context) error {
	_, err := t.control(ctx, "transaction.discard", nil)
	return err
}

// Watch marks keys for optimistic locking: if any of them changes before Exec,
// the transaction aborts and nothing is applied. Call it before Multi.
func (t *Tx) Watch(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return &SynapError{Message: "synap: Watch requires at least one key", Code: "InvalidArgument"}
	}
	_, err := t.control(ctx, "transaction.watch", map[string]interface{}{"keys": keys})
	return err
}

// Unwatch clears every key this transaction was watching.
func (t *Tx) Unwatch(ctx context.Context) error {
	_, err := t.control(ctx, "transaction.unwatch", nil)
	return err
}

// Exec applies the queued commands atomically and closes the transaction.
// A transaction whose watched key changed reports Aborted with no results.
func (t *Tx) Exec(ctx context.Context) (ExecResult, error) {
	raw, err := t.control(ctx, "transaction.exec", nil)
	if err != nil {
		return ExecResult{}, err
	}

	var result ExecResult
	if err := raw.Decode(&result); err != nil {
		return ExecResult{}, newInvalidResponseError("transaction.exec: " + err.Error())
	}
	return result, nil
}

// control issues a transaction command, which carries the client id as its own
// field rather than through the TXQUEUE wrapper the write path uses.
func (t *Tx) control(ctx context.Context, command string, extra map[string]interface{}) (response, error) {
	payload := map[string]interface{}{"client_id": t.clientID}
	for k, v := range extra {
		payload[k] = v
	}
	return t.client.sendCommand(ctx, command, payload)
}
