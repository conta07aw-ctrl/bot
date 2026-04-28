/**
 * PUSD Approval — ensures the Polymarket CTF Exchange contracts are approved
 * to spend PUSD (Polymarket USD) from the user's proxy wallet.
 *
 * On Polymarket, the proxy wallet needs to approve:
 *   - NegRiskCtfExchange (for 15-min crypto markets)
 *   - ConditionalTokens (CTF) contract
 *
 * This is a one-time setup per wallet. Called when user first saves their keys.
 *
 * Note: For proxy wallets, the approval is typically already done by Polymarket
 * when the proxy wallet is created. This service checks and approves if needed.
 */

const { ethers } = require('ethers');
const { HttpsProxyAgent } = require('https-proxy-agent');

const POLYGON_RPC = 'https://polygon.llamarpc.com';

// Contracts on Polygon
const PUSD_ADDRESS = '0x455e53CBB86018Ac2B8092FdCd39c5b8118FA047';
const NEG_RISK_CTF_EXCHANGE = '0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E';
const CTF_CONTRACT = '0x4D97DCd97eC945f40cF65F87097ACe5EA0476045';
const NEG_RISK_ADAPTER = '0xC5d563A36AE78145C45a50134d48A1215220f80a';

const PUSD_ABI = [
  'function allowance(address owner, address spender) view returns (uint256)',
  'function approve(address spender, uint256 amount) returns (bool)',
];

const CTF_ABI = [
  'function isApprovedForAll(address owner, address operator) view returns (bool)',
  'function setApprovalForAll(address operator, bool approved)',
];

const MAX_UINT256 = ethers.MaxUint256;

/**
 * Check and approve PUSD + CTF spending for Polymarket trading.
 *
 * @param {string} privateKey - Signer wallet private key
 * @param {string} funderAddress - Proxy wallet address (if different from signer)
 * @returns {{ pusd: boolean, ctf: boolean, isProxy: boolean }}
 */
async function checkAndApprove(privateKey, funderAddress) {
  const providerOptions = process.env.POLY_PROXY_URL ? { httpsAgent: new HttpsProxyAgent(process.env.POLY_PROXY_URL) } : {};
  const provider = new ethers.JsonRpcProvider(POLYGON_RPC, 137, {
    staticNetwork: true,
    ...providerOptions,
  });
  const wallet = new ethers.Wallet(privateKey, provider);
  const signerAddr = wallet.address;
  const isProxy = funderAddress && funderAddress.toLowerCase() !== signerAddr.toLowerCase();

  // For proxy wallets, Polymarket handles approvals — we just check
  // For EOA wallets, we need to approve ourselves
  const checkAddr = isProxy ? funderAddress : signerAddr;

  const pusd = new ethers.Contract(PUSD_ADDRESS, PUSD_ABI, wallet);

  const result = { pusd: false, ctf: false, isProxy };

  try {
    // Check PUSD allowance for NegRiskCtfExchange
    const allowance = await pusd.allowance(checkAddr, NEG_RISK_CTF_EXCHANGE);
    const threshold = ethers.parseUnits('1000000', 6); // 1M PUSD threshold

    if (allowance < threshold) {
      if (isProxy) {
        console.log('[PUSD Approval] proxy wallet PUSD allowance low — Polymarket should handle this');
        result.pusd = false;
      } else {
        console.log('[PUSD Approval] approving PUSD for NegRiskCtfExchange...');
        const tx = await pusd.approve(NEG_RISK_CTF_EXCHANGE, MAX_UINT256);
        await tx.wait();
        console.log('[PUSD Approval] PUSD approved — tx:', tx.hash);
        result.pusd = true;

        // Also approve for NegRiskAdapter
        const tx2 = await pusd.approve(NEG_RISK_ADAPTER, MAX_UINT256);
        await tx2.wait();
        console.log('[PUSD Approval] PUSD approved for NegRiskAdapter — tx:', tx2.hash);
      }
    } else {
      console.log('[PUSD Approval] PUSD already approved');
      result.pusd = true;
    }

    // Check CTF approval (for conditional token operations)
    const ctf = new ethers.Contract(CTF_CONTRACT, CTF_ABI, wallet);
    const isApproved = await ctf.isApprovedForAll(checkAddr, NEG_RISK_CTF_EXCHANGE);

    if (!isApproved) {
      if (isProxy) {
        console.log('[PUSD Approval] proxy wallet CTF not approved — Polymarket should handle this');
        result.ctf = false;
      } else {
        console.log('[PUSD Approval] approving CTF for NegRiskCtfExchange...');
        const tx = await ctf.setApprovalForAll(NEG_RISK_CTF_EXCHANGE, true);
        await tx.wait();
        console.log('[PUSD Approval] CTF approved — tx:', tx.hash);
        result.ctf = true;
      }
    } else {
      console.log('[PUSD Approval] CTF already approved');
      result.ctf = true;
    }
  } catch (err) {
    console.error('[PUSD Approval] error:', err.message);
  }

  return result;
}

module.exports = { checkAndApprove };
