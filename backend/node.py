"""Node orchestration: ties the chain, pool, wallets, mining, and P2P together.

A :class:`Node` owns a single blockchain replica, a mempool, a local wallet,
and a peer list.  It can mine (foreground or in a background thread), broadcast
new blocks/transactions to its peers, and synchronise its chain from whichever
peer has the greatest cumulative work — resolving forks by re-organisation.
"""

import threading
import time

from . import crypto, pow as pow_mod
from .block import Block
from .blockchain import Blockchain
from .config import (COINBASE_REWARD, CONTRACT_EVENT_DEDUP_KEY,
                     TUNABLE_PARAM_SPECS, coerce_param, tunable_defaults,
                     validate_param)
from .p2p import PeerRegistry, dial_peer, http_get_json, http_post_json
from .state import ZERO_ADDRESS
from .storage import DataPaths, atomic_write_json, read_json
from .transaction import (Transaction, create_call, create_coinbase,
                          create_deploy, create_transfer)
from .txpool import TxPool
from .wallet_store import WalletStore


class Node:
    def __init__(self, cfg):
        self.cfg = cfg
        self.node_id = cfg.get("node_id", "node")
        self.port = cfg.get("port", 8000)
        self.host = cfg.get("host", "127.0.0.1")
        self.started_at = time.time()

        self.paths = DataPaths(cfg.get("data_dir", "data"), cfg)
        self.paths.ensure()
        self.blockchain = Blockchain(cfg, self.paths)
        self.txpool = TxPool(max_size=cfg.get("MAX_TX_PER_BLOCK", 1000))
        self.wallets = WalletStore(self.paths.wallets_path)
        self.peers = PeerRegistry()

        self._mining = False
        self._mine_thread = None
        self._lock = threading.RLock()
        # Set when mining should wake early (stop requested, or the interval
        # parameter changed) so parameter updates take effect on the very next
        # wait instead of after an in-progress sleep completes.
        self._mining_wake = threading.Event()
        self._logs = []
        self.hashrate = 0.0
        self._last_mine_duration = 0.0
        self._last_mine_attempts = 0

        # Node-local tunable parameters (operational, non-consensus).  Values
        # persist across restarts in settings.json; every accepted change is
        # appended to param_history.json for audit.
        self._param_history = []
        self._load_param_settings()
        # The pool is constructed before saved settings are loaded; bring its
        # admission cap in line with the (possibly persisted) tunable value.
        self.txpool.set_max_size(int(self.cfg.get("MAX_TX_PER_BLOCK", 200)))

    # ==================================================================== #
    # Lifecycle
    # ==================================================================== #
    def start(self):
        self.blockchain.load()
        self._load_txpool()
        self._register_configured_peers()
        self.log("info", f"node {self.node_id} started on port {self.port}, "
                         f"height {self.blockchain.height}")
        if self.cfg.get("mine"):
            self.start_mining()

    def _load_txpool(self):
        data = read_json(self.paths.txpool_path, [])
        self.txpool.load(data, self.blockchain.state)

    def save_txpool(self):
        atomic_write_json(self.paths.txpool_path, self.txpool.to_list())

    def _register_configured_peers(self):
        for peer in self.cfg.get("peers", []):
            if ":" in peer:
                host, _, port = peer.partition(":")
                self.peers.add(None, host or "127.0.0.1", int(port or 8000))

    # ==================================================================== #
    # Tunable parameters (node-local, non-consensus)
    # ==================================================================== #
    PARAM_HISTORY_LIMIT = 500

    def _load_param_settings(self):
        """Overlay persisted tunable values onto the startup configuration."""
        self._param_history = read_json(self.paths.param_history_path, [])
        saved = read_json(self.paths.settings_path, {}) or {}
        for key, spec in TUNABLE_PARAM_SPECS.items():
            if key not in saved:
                continue
            try:
                value = coerce_param(key, saved[key])
            except (TypeError, ValueError):
                self.log("warn", f"忽略已保存的非法参数值: {key}={saved[key]!r}")
                continue
            ok, _msg = validate_param(key, value)
            if not ok:
                self.log("warn", f"忽略超出范围的已保存参数: {key}={value!r}")
                continue
            self.cfg[key] = value
        # Keep the historical lowercase alias used by the mining loop in sync.
        self.cfg["mining_interval"] = self.cfg["MINING_INTERVAL"]

    def tunable_params(self):
        """Return current/default/range metadata for every tunable parameter."""
        out = []
        for key, spec in TUNABLE_PARAM_SPECS.items():
            out.append({
                "key": key,
                "label": spec["label"],
                "unit": spec["unit"],
                "type": spec["type"],
                "step": spec["step"],
                "help": spec.get("help", ""),
                "default": spec["default"],
                "min": spec["min"],
                "max": spec["max"],
                "current": self.cfg.get(key, spec["default"]),
                "is_default": self.cfg.get(key) == spec["default"],
            })
        return out

    def _record_param_change(self, key, old, new, source="ui"):
        entry = {"time": time.time(), "key": key, "old": old, "new": new,
                 "source": source}
        self._param_history.append(entry)
        self._param_history = self._param_history[-self.PARAM_HISTORY_LIMIT:]
        atomic_write_json(self.paths.param_history_path, self._param_history)
        return entry

    def param_history(self, limit=100):
        return list(reversed(self._param_history[-limit:]))

    def update_params(self, updates, source="ui"):
        """Validate and atomically apply a ``{key: value}`` parameter batch.

        Every value is range-checked first; if any is invalid nothing is
        applied, persisted, or recorded.  Returns ``(applied, errors)`` where
        ``applied`` is the list of audit entries for changed values.
        """
        if not isinstance(updates, dict):
            return [], {"_": "请求格式必须是参数键值映射"}
        # Phase 1: reject unknown keys and coerce/validate everything before
        # touching any live state, so an out-of-range value can never leave a
        # half-applied batch.
        coerced, errors = {}, {}
        for key, raw in updates.items():
            if key not in TUNABLE_PARAM_SPECS:
                errors[key] = "未知参数"
                continue
            try:
                value = coerce_param(key, raw)
            except (TypeError, ValueError) as e:
                errors[key] = str(e)
                continue
            ok, msg = validate_param(key, value)
            if not ok:
                errors[key] = msg
                continue
            coerced[key] = value
        if errors:
            return [], errors

        # Phase 2: apply, persist, and audit (under one lock with the mining
        # thread so reads always see a consistent set).
        applied = []
        with self._lock:
            for key, value in coerced.items():
                old = self.cfg.get(key, TUNABLE_PARAM_SPECS[key]["default"])
                if old == value:
                    continue
                self.cfg[key] = value
                if key == "MINING_INTERVAL":
                    self.cfg["mining_interval"] = value
                entry = self._record_param_change(key, old, value, source)
                applied.append(entry)
            if applied:
                persisted = {k: self.cfg[k] for k in TUNABLE_PARAM_SPECS}
                atomic_write_json(self.paths.settings_path, persisted)
                for e in applied:
                    self.log("info",
                             f"参数 {e['key']} 已从 {e['old']:g} 调整为 {e['new']:g}")
            if any(e["key"] == "MAX_TX_PER_BLOCK" for e in applied):
                # Applies to subsequent admissions / block packaging immediately.
                self.txpool.set_max_size(int(self.cfg["MAX_TX_PER_BLOCK"]))
            # Short-circuit any in-progress cadence wait so the new interval
            # governs the very next block; the node/thread never restarts.
            if any(e["key"] == "MINING_INTERVAL" for e in applied) and self._mining:
                self._mining_wake.set()
        return applied, {}

    def reset_params(self, source="ui"):
        """Restore every tunable parameter to its built-in default.

        Only values that actually differ generate audit entries, so a reset
        when everything is already at the default is a harmless no-op.
        """
        return self.update_params(tunable_defaults(), source=source)

    # ==================================================================== #
    # Logging
    # ==================================================================== #
    def log(self, level, message):
        entry = {"time": time.time(), "level": level, "message": str(message)}
        self._logs.append(entry)
        if len(self._logs) > 500:
            self._logs = self._logs[-500:]
        atomic_write_json(self.paths.logs_path, self._logs)

    def logs(self, limit=100):
        return list(reversed(self._logs[-limit:]))

    # ==================================================================== #
    # Mining
    # ==================================================================== #
    def mine_block(self, miner_address=None, wait=False):
        """Pack the mempool into a candidate block and mine it (foreground)."""
        with self._lock:
            miner = miner_address or (self.wallets.list()[0]["address"]
                                      if self.wallets.list() else ZERO_ADDRESS)
            tx_cap = int(self.cfg.get("MAX_TX_PER_BLOCK", 200))
            candidates = self.txpool.all()[:tx_cap]
            index = self.blockchain.height + 1
            coinbase = create_coinbase(miner, COINBASE_REWARD, index)
            txs = [coinbase] + candidates
            block = Block(index, self.blockchain.head.hash, txs)
            # Difficulty must be computed from the block's own timestamp.
            block.difficulty = pow_mod.next_difficulty(self.blockchain, block)
            block._recompute_header()
            # Compute the state root that this block will commit to.
            new_state = self.blockchain.state.copy()
            new_state, _receipts = self.blockchain.apply_block(block, new_state)
            block.set_state_root(new_state.root())

            started = time.time()
            attempts = pow_mod.mine(block, block.difficulty)
            elapsed = time.time() - started
            self._last_mine_duration = elapsed
            self._last_mine_attempts = attempts
            if elapsed > 0:
                self.hashrate = pow_mod.effective_hashrate(attempts, elapsed)

            status, message = self.blockchain.add_block(block)
            if status == "extended":
                included = {tx.txid for tx in candidates}
                self.txpool.remove_many(included)
                self.save_txpool()
                self._record_events(self.blockchain.last_receipts, block.index)
                self.sync_contract_files()
                self.log("info", f"mined block #{block.index} "
                                 f"({attempts} hashes in {elapsed:.2f}s)")
                self.broadcast_block(block)
            else:
                self.log("warn", f"mined block rejected: {message}")
            return status, message, block.index

    def start_mining(self):
        if self._mining:
            return False
        self._mining = True
        self._mining_wake.clear()
        self._mine_thread = threading.Thread(target=self._mining_loop,
                                             daemon=True)
        self._mine_thread.start()
        self.log("info", "auto-mining started")
        return True

    def stop_mining(self):
        self._mining = False
        self._mining_wake.set()
        self.log("info", "auto-mining stopped")
        return True

    def _mining_loop(self):
        # The cadence is re-read (with the event's remaining-time cleared) on
        # every pass, so saving a new MINING_INTERVAL affects the upcoming
        # blocks immediately without restarting the node.
        while self._mining:
            interval = self.cfg.get("MINING_INTERVAL",
                                   self.cfg.get("mining_interval", 3.0))
            try:
                self.mine_block()
            except Exception as e:  # noqa: BLE001
                self.log("error", f"mining error: {e}")
            # Interruptible wait: a parameter save wakes the loop, after which
            # the new interval is honoured; otherwise we sleep the full cadence.
            self._mining_wake.wait(timeout=max(0.0, float(interval)))
            self._mining_wake.clear()

    @property
    def mining(self):
        return self._mining

    def reported_hashrate(self):
        return self.hashrate

    def pending_transactions(self):
        return self.txpool.ordered_all()

    def contract_events(self, address):
        return read_json(self.paths.contract_path(address), {}).get("events", [])

    # ==================================================================== #
    # Transactions
    # ==================================================================== #
    def submit_transaction(self, tx, broadcast=True):
        """Validate and admit a transaction; return ``(ok, reason)``."""
        ok, reason = self.txpool.validate(tx, self.blockchain.state)
        if not ok:
            return False, reason
        self.txpool.add(tx)
        self.save_txpool()
        self.log("info", f"tx accepted into pool: {tx.txid[:16]}…")
        if broadcast:
            self.broadcast_tx(tx)
        return True, "accepted"

    def sign_tx_with_wallet(self, tx):
        priv = self.wallets.private_key(tx.sender)
        if priv is None:
            return False, "sender wallet not found on this node"
        tx.sign_with(priv)
        return True, "signed"

    def create_transfer(self, sender, to, amount, fee=0.0):
        nonce = self.blockchain.state.nonce(sender)
        tx = create_transfer(sender, to, amount, fee, nonce)
        ok, reason = self.sign_tx_with_wallet(tx)
        if not ok:
            return None, reason
        return tx, None

    def create_deploy(self, sender, code, fee=0.0, constructor=None):
        nonce = self.blockchain.state.nonce(sender)
        tx = create_deploy(sender, code, fee, nonce, constructor=constructor)
        ok, reason = self.sign_tx_with_wallet(tx)
        if not ok:
            return None, reason
        return tx, None

    def create_call(self, sender, contract, function, args, fee=0.0, value=0):
        nonce = self.blockchain.state.nonce(sender)
        tx = create_call(sender, contract, function, args, fee, nonce,
                         value=value)
        ok, reason = self.sign_tx_with_wallet(tx)
        if not ok:
            return None, reason
        return tx, None

    # ==================================================================== #
    # P2P: broadcast + sync
    # ==================================================================== #
    def broadcast_block(self, block):
        payload = block.to_dict()
        for peer in self.peers.all():
            try:
                http_post_json(f"{peer.url}/p2p/block", payload, timeout=3)
            except Exception as e:  # noqa: BLE001
                peer.last_error = str(e)[:120]

    def broadcast_tx(self, tx):
        payload = tx.to_dict()
        for peer in self.peers.all():
            try:
                http_post_json(f"{peer.url}/p2p/tx", payload, timeout=3)
            except Exception as e:  # noqa: BLE001
                peer.last_error = str(e)[:120]

    def receive_block(self, block_dict):
        block = Block.from_dict(block_dict)
        status, message = self.blockchain.add_block(block)
        if status == "extended":
            included = {tx.txid for tx in block.transactions}
            self.txpool.remove_many(included)
            self.save_txpool()
            self._record_events(self.blockchain.last_receipts, block.index)
            self.sync_contract_files()
            self.log("info", f"received block #{block.index} from peer")
            self.broadcast_block(block)
        elif status == "reorg":
            self._readmit_abandoned()
            self.sync_contract_files()
            self.log("warn", f"reorg: {message}")
        return status, message

    def receive_tx(self, tx_dict):
        tx = Transaction.from_dict(tx_dict)
        ok, reason = self.submit_transaction(tx, broadcast=True)
        return ok, reason

    def _readmit_abandoned(self):
        for blk in self.blockchain.last_abandoned:
            for tx in blk.transactions:
                if not tx.is_coinbase():
                    self.txpool.re_admit([tx])
        self.save_txpool()

    def _record_events(self, receipts, height):
        """Append contract events from ``receipts`` to per-contract files."""
        for r in receipts:
            addr = r.get("contract")
            events = r.get("events") or []
            if not addr or not events:
                continue
            path = self.paths.contract_path(addr)
            data = read_json(path, {"address": addr, "events": []})
            for e in events:
                data.setdefault("events", []).append({
                    "height": height, "txid": r.get("txid"),
                    "event": e.get("event"), "data": e.get("data"),
                })
            data["events"] = data["events"][-2000:]
            dedup = {}
            for entry in data["events"]:
                dedup[entry.get(CONTRACT_EVENT_DEDUP_KEY)] = entry
            data["events"] = list(dedup.values())
            atomic_write_json(path, data)

    def sync_contract_files(self):
        """Refresh per-contract files from the current world state."""
        st = self.blockchain.state
        for addr, c in st.contracts.items():
            path = self.paths.contract_path(addr)
            existing = read_json(path, {})
            data = {
                "address": addr,
                "creator": c.get("creator"),
                "code": c.get("code"),
                "storage": c.get("storage"),
                "balance": st.balance(addr),
                "events": existing.get("events", []),
            }
            atomic_write_json(path, data)

    def sync_with_peers(self):
        """Pull the heaviest known chain from our peers.  Returns a summary."""
        summary = {"checked": 0, "synced_blocks": 0, "reorgs": 0,
                   "best_peer": None, "errors": []}
        best = None
        for peer in self.peers.all():
            up, data = dial_peer(peer)
            summary["checked"] += 1
            if up and data.get("chainwork", 0) > self.blockchain.chainwork:
                if best is None or data["chainwork"] > best[1].get("chainwork", 0):
                    best = (peer, data)
        if best is None:
            summary["errors"].append("no peer with a heavier chain")
            return summary

        peer, data = best
        summary["best_peer"] = peer.id
        target_height = data.get("height", 0)
        next_h = self.blockchain.height + 1
        while next_h <= target_height:
            try:
                batch = http_get_json(
                    f"{peer.url}/p2p/blocks?from={next_h}&to="
                    f"{min(next_h + 100, target_height)}", timeout=5)
            except Exception as e:  # noqa: BLE001
                summary["errors"].append(f"sync failed at height {next_h}: {e}")
                break
            for bd in batch.get("blocks", []):
                status, message = self.receive_block(bd)
                if status == "reorg":
                    summary["reorgs"] += 1
                if status in ("extended", "reorg"):
                    summary["synced_blocks"] += 1
            next_h = self.blockchain.height + 1
        self.save_txpool()
        self.log("info", f"sync complete: +{summary['synced_blocks']} blocks "
                         f"from {peer.id}")
        return summary

    def announce(self):
        """Ask peers to consider us (used by non-seed nodes on startup)."""
        for peer in self.peers.all():
            try:
                http_post_json(f"{peer.url}/p2p/announce", {
                    "id": self.node_id, "host": self.host, "port": self.port,
                }, timeout=3)
            except Exception:  # noqa: BLE001
                pass
