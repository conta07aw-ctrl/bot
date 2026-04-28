/**
 * Telegram notification service — PER-USER notifications.
 *
 * Each user configures their own bot token + chat ID in the dashboard.
 * Notifications are sent individually to each user who has it enabled.
 *
 * Setup per user:
 *   1. Create a bot via @BotFather on Telegram → get bot token
 *   2. Send /start to your bot
 *   3. Get chat ID via https://api.telegram.org/bot<TOKEN>/getUpdates
 *   4. Enter bot token + chat ID in dashboard Settings → Telegram
 */

const axios = require('axios');
const userStore = require('../store/users');

const API_BASE = 'https://api.telegram.org/bot';

class TelegramNotifier {
  constructor() {
    console.log('[Telegram] per-user notifications ready');
  }

  /**
   * Send a message to a specific user (if they have Telegram configured + enabled).
   */
  async sendToUser(username, text) {
    try {
      const settings = userStore.getSettings(username);
      if (!settings?.telegramEnabled || !settings?.telegramBotToken || !settings?.telegramChatId) return;

      await axios.post(`${API_BASE}${settings.telegramBotToken}/sendMessage`, {
        chat_id: settings.telegramChatId,
        text,
        parse_mode: 'HTML',
        disable_web_page_preview: true,
      }, { timeout: 10_000 });
    } catch (err) {
      console.debug(`[Telegram] send to ${username} failed:`, err.message);
    }
  }

  /**
   * Broadcast a message to ALL users who have Telegram enabled.
   */
  async broadcast(text) {
    const users = userStore.listUsers();
    for (const username of users) {
      this.sendToUser(username, text).catch(() => {});
    }
  }

  async notifyTrade(trade) {
    const icon = trade.dryRun ? '🧪' : trade.success ? '✅' : trade.hedged ? '🛡️' : '❌';
    const status = trade.dryRun ? 'DRY-RUN' : trade.success ? 'FILLED' : trade.hedged ? 'HEDGED' : 'FAILED';

    const msg = `${icon} <b>${status}</b> — ${trade.asset} Leg${trade.leg}\n` +
      `Kalshi: ${trade.kalshiSide}@${trade.kalshiPrice}\n` +
      `Poly: ${trade.polySide}@${trade.polyPrice}\n` +
      `Units: ${trade.units} | ROI: ${trade.expectedRoi}%`;

    await this.broadcast(msg);
  }

  async notifyHedge(hedge) {
    const ok = hedge.hedgeAttempts?.every((a) => a.success);
    const msg = `🛡️ <b>EMERGENCY HEDGE</b> ${ok ? 'SUCCESS' : 'FAILED'}\n` +
      `Asset: ${hedge.asset} Leg${hedge.leg}\n` +
      `Poly filled: ${hedge.polyFilled}, Kalshi filled: ${hedge.kalshiFilled}\n` +
      `Attempts: ${hedge.hedgeAttempts?.length || 0}`;

    await this.broadcast(msg);
  }

  async notifyBalanceWarning(reason) {
    await this.broadcast(`⚠️ <b>CAPITAL GUARD</b>\n${reason}\nTrading paused.`);
  }

  async notifyRedeem(entry) {
    // Only notify the specific user who got redeemed
    await this.sendToUser(entry.username, `💰 <b>AUTO-REDEEM</b>\nAmount: ${entry.balance}\nMethod: ${entry.method}`);
  }
}

module.exports = TelegramNotifier;
