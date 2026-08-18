"""
Heartbeat loop — main orchestration per heartbeat.md.
State machine: setup → join → play → settle → repeat.
Modified: Bypassed First-Run Intake and Wallet Validations for Free Room grinding.
"""
import os
import asyncio
from bot.api_client import MoltyAPI, APIError
from bot.dashboard.state import dashboard_state
from bot.state_router import determine_state, NO_ACCOUNT, NO_IDENTITY, IN_GAME, READY_PAID, READY_FREE
from bot.setup.account_setup import ensure_account_ready
from bot.setup.wallet_setup import ensure_molty_wallet
from bot.setup.whitelist import ensure_whitelist
from bot.setup.identity import ensure_identity
from bot.game.room_selector import select_room
from bot.game.free_join import join_free_game
from bot.game.paid_join import join_paid_game
from bot.game.websocket_engine import WebSocketEngine
from bot.game.settlement import settle_game
from bot.memory.agent_memory import AgentMemory
from bot.credentials import load_credentials, get_api_key
from bot.config import (
    ADVANCED_MODE, ROOM_MODE, AUTO_WHITELIST,
    AUTO_SC_WALLET, ENABLE_MEMORY, AUTO_IDENTITY,
)
from bot.utils.logger import get_logger

log = get_logger(__name__)


class Heartbeat:
    """Main heartbeat loop — runs forever, manages the full agent lifecycle."""

    def __init__(self):
        self.api: MoltyAPI | None = None
        self.memory = AgentMemory()
        self.running = True
        self._agent_key = "agent-1"  # Consistent dashboard key
        self._agent_name = "Agent"

    async def run(self):
        """Entry point — runs the heartbeat loop indefinitely."""
        log.info("═══════════════════════════════════════════")
        log.info("  MOLTY ROYALE AI AGENT — STARTING (FREE MODE)")
        log.info("═══════════════════════════════════════════")

        # Log active config
        log.info("Config:")
        log.info("  ROOM_MODE       = %s", ROOM_MODE)
        log.info("  ENABLE_MEMORY   = %s", ENABLE_MEMORY)
        log.info("  AUTO_SETUP      = BYPASSED")

        # Phase 0: Langsung comot API Key dari .env, lompati validasi akun.
        api_key = os.getenv("API_KEY") or os.getenv("CLAW_API_KEY")
        if not api_key:
            log.error("❌ API_KEY belum diisi di .env! Bot dihentikan.")
            self.running = False
            return

        self.api = MoltyAPI(api_key)
        creds = {"agent_name": "Otooong"} # Default mock data

        if not self.running:
            return

        # Feed dashboard
        dashboard_state.bots_running = 1
        dashboard_state.add_log("Bot started in Free Mode", "info")

        # Load memory (if enabled)
        if ENABLE_MEMORY:
            await self.memory.load()
            if creds.get("agent_name"):
                self.memory.set_agent_name(creds["agent_name"])
        else:
            log.info("Memory system disabled (ENABLE_MEMORY=false)")

        # Main loop — NEVER exits, NEVER crashes
        consecutive_errors = 0
        while self.running:
            try:
                await self._heartbeat_cycle()
                consecutive_errors = 0  # Reset on success
            except KeyboardInterrupt:
                log.info("Shutdown requested")
                self.running = False
            except Exception as e:
                consecutive_errors += 1
                # Escalating backoff: 10s → 30s → 60s → 120s
                wait = min(10 * (2 ** min(consecutive_errors - 1, 4)), 120)
                log.error("Heartbeat error (#%d): %s. Retrying in %ds...",
                          consecutive_errors, e, wait)
                await asyncio.sleep(wait)

        if self.api:
            await self.api.close()
        log.info("Agent stopped.")

    async def _heartbeat_cycle(self):
        """Single heartbeat cycle: check state → route → act."""
        # Step 1: GET /accounts/me
        try:
            me = await self.api.get_accounts_me()
        except APIError as e:
            if e.status == 401:
                log.error("Invalid API key. Re-run setup.")
                self.running = False
                return
            raise

        # Step 2: Determine state
        state, ctx = determine_state(me)
        log.info("State: %s", state)

        # Feed dashboard with account info — use CONSISTENT key
        self._agent_key = str(me.get("agentId", me.get("id", "agent-1")))
        self._agent_name = me.get("agentName", me.get("name", "Agent"))
        balance = me.get("balance", 0)
        dashboard_state.total_smoltz = balance
        dashboard_state.update_agent(self._agent_key, {
            "name": self._agent_name,
            "status": "playing" if state == IN_GAME else "idle",
            "smoltz": balance,
            "whitelisted": state != NO_IDENTITY,
        })

        # Step 3: Route based on state
        if state == NO_IDENTITY:
            await self._handle_no_identity(me)
            return

        if state == IN_GAME:
            await self._handle_in_game(ctx)
            return

        if state in (READY_FREE, READY_PAID):
            await self._handle_ready(me, state)
            return

    async def _handle_no_identity(self, me: dict):
        """Identitas Web3 opsional untuk Free Room. Lewati setup dompet."""
        log.info("Identitas Web3 tidak ditemukan, tapi Free Room tidak butuh identitas. Melanjutkan siklus...")
        await asyncio.sleep(2)
        return

    async def _handle_ready(self, me: dict, state: str):
        """Join a game based on room selection."""
        room_type = select_room(me)

        try:
            if room_type == "paid":
                game_id, agent_id = await join_paid_game(self.api)
            else:
                game_id, agent_id = await join_free_game(self.api)
        except APIError as e:
            if e.code == "NO_IDENTITY":
                log.error("Identity required. Will setup next cycle.")
                return
            log.warning("Join failed: %s. Retrying in 10s.", e)
            await asyncio.sleep(10)
            return
        except RuntimeError as e:
            log.warning("Join failed: %s. Retrying in 10s.", e)
            await asyncio.sleep(10)
            return

        # Successfully joined → play
        await self._play_game(game_id, agent_id, room_type)

    async def _handle_in_game(self, ctx: dict):
        """Resume or start playing an active game.
        Per game-loop.md: always connect WS, even when dead.
        Dead agents wait for game_ended inside the WS engine.
        """
        game_id = ctx["game_id"]
        agent_id = ctx["agent_id"]
        entry_type = ctx.get("entry_type", "free")

        if not ctx.get("is_alive", True):
            log.info("Agent is dead in game %s. Connecting WS to wait for game_ended.", game_id)

        await self._play_game(game_id, agent_id, entry_type)

    async def _play_game(self, game_id: str, agent_id: str, entry_type: str):
        """Run the WebSocket gameplay engine."""
        log.info("═══ PLAYING GAME: %s (type=%s) ═══", game_id, entry_type)

        # Feed dashboard — use SAME key as heartbeat so no duplicate card
        dashboard_state.update_agent(self._agent_key, {
            "status": "playing",
            "room_id": game_id,
            "room_name": entry_type + " room",
        })
        dashboard_state.add_log(f"Joined {entry_type} game: {game_id[:12]}", "info", self._agent_key)

        # Set temp memory for this game
        self.memory.set_temp_game(game_id)
        await self.memory.save()

        # Run WebSocket engine — pass agent_key + name for dashboard
        engine = WebSocketEngine(game_id, agent_id)
        engine.dashboard_key = self._agent_key
        engine.dashboard_name = self._agent_name
        game_result = await engine.run()

        # Settle
        await settle_game(game_result, entry_type, self.memory)

        log.info("Game complete. Starting next cycle in 5s...")
        await asyncio.sleep(5)