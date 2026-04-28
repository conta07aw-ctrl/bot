/**
 * Test script: place a REAL 1-contract order on Kalshi and log the RAW response.
 *
 * Usage: node test-kalshi-order.js
 *
 * This will:
 * 1. Load keys from the user store
 * 2. Connect to Kalshi
 * 3. Find an active 15-min market
 * 4. Place a BUY market order for 1 contract at 1¢ (cheapest possible)
 * 5. Log the FULL RAW response from the API
 */

const config = require('./src/config');
const KalshiConnector = require('./src/connectors/kalshi');
const users = require('./src/store/users');

async function main() {
  console.log('=== KALSHI ORDER TEST ===\n');

  // 1. Load keys
  const userIds = users.listUsers();
  if (userIds.length === 0) {
    console.error('No users found in store');
    process.exit(1);
  }
  const keys = users.getKeys(userIds[0]);
  console.log(`User: ${userIds[0]}`);
  console.log(`API Key: ${keys.kalshiApiKey?.slice(0, 8)}...`);

  // 2. Connect
  const kalshi = new KalshiConnector();
  kalshi.apiKey = keys.kalshiApiKey;
  kalshi.privateKey = keys.kalshiPrivateKeyPem;
  await kalshi.connect();
  console.log(`Connected: ${kalshi.connected}\n`);

  if (!kalshi.connected) {
    console.error('Failed to connect');
    process.exit(1);
  }

  // 2.5. Check balance first
  try {
    const balance = await kalshi._get('/trade-api/v2/portfolio/balance');
    console.log('=== CURRENT BALANCE ===');
    console.log(JSON.stringify(balance, null, 2));
    console.log('');
  } catch (e) {
    console.error('Balance check failed:', e.message);
  }

  // 3. Find an active market
  try {
    const markets = await kalshi._get('/trade-api/v2/markets?status=open&series_ticker=KXBTC15M&limit=5');
    if (!markets.markets || markets.markets.length === 0) {
      console.error('No active BTC 15-min markets found');
      process.exit(1);
    }
    const market = markets.markets[0];
    const ticker = market.ticker;
    console.log(`=== ACTIVE MARKET ===`);
    console.log(`Ticker: ${ticker}`);
    console.log(`Title: ${market.title || market.subtitle}`);
    console.log(`Yes Ask: ${market.yes_ask}¢  No Ask: ${market.no_ask}¢`);
    console.log('');

    // 4. Get orderbook to see real prices
    const book = await kalshi._get(`/trade-api/v2/markets/${ticker}/orderbook`);
    console.log('=== ORDERBOOK (top 3) ===');
    const yesLevels = (book.orderbook?.yes || []).slice(0, 3);
    const noLevels = (book.orderbook?.no || []).slice(0, 3);
    console.log('YES:', JSON.stringify(yesLevels));
    console.log('NO:', JSON.stringify(noLevels));
    console.log('');

    // 5. Place a TEST order: BUY YES at 1¢ (will almost certainly NOT fill — too cheap)
    // This tests the API response format without risking real money
    console.log('=== TEST 1: LIMIT BUY YES @1¢ x1 (should NOT fill — price too low) ===');
    const limitBody = {
      ticker,
      side: 'yes',
      action: 'buy',
      type: 'limit',
      count: 1,
      yes_price: 1,
      client_order_id: `test-limit-${Date.now()}`,
    };
    console.log('Request body:', JSON.stringify(limitBody, null, 2));

    const limitResult = await kalshi._post('/trade-api/v2/portfolio/orders', limitBody);
    console.log('\n=== RAW LIMIT RESPONSE ===');
    console.log(JSON.stringify(limitResult, null, 2));

    // Cancel it if it's resting
    if (limitResult.order?.order_id) {
      console.log(`\nCancelling test order ${limitResult.order.order_id}...`);
      try {
        await kalshi._delete(`/trade-api/v2/portfolio/orders/${limitResult.order.order_id}`);
        console.log('Cancelled OK');
      } catch (e) {
        console.log('Cancel result:', e.response?.data || e.message);
      }
    }

    // 6. Place a MARKET order at 1¢ max cost (cheapest test)
    console.log('\n=== TEST 2: MARKET BUY YES @1¢ maxCost=1¢ x1 ===');
    const marketBody = {
      ticker,
      side: 'yes',
      action: 'buy',
      type: 'market',
      count: 1,
      yes_price: 1,
      buy_max_cost: 1,
      client_order_id: `test-market-${Date.now()}`,
    };
    console.log('Request body:', JSON.stringify(marketBody, null, 2));

    try {
      const marketResult = await kalshi._post('/trade-api/v2/portfolio/orders', marketBody);
      console.log('\n=== RAW MARKET RESPONSE ===');
      console.log(JSON.stringify(marketResult, null, 2));
    } catch (e) {
      console.log('\n=== MARKET ORDER ERROR ===');
      console.log(JSON.stringify(e.response?.data || { message: e.message }, null, 2));
    }

    // 7. Now test with a real fillable price (if user confirms)
    // Find the best yes ask from the book
    const bestYesPrice = yesLevels.length > 0 ? yesLevels[0][0] : null;
    if (bestYesPrice && bestYesPrice <= 20) {
      console.log(`\n=== TEST 3: MARKET BUY YES @${bestYesPrice}¢ maxCost=${bestYesPrice}¢ x1 (REAL — will spend ${bestYesPrice}¢) ===`);
      const realBody = {
        ticker,
        side: 'yes',
        action: 'buy',
        type: 'market',
        count: 1,
        yes_price: bestYesPrice,
        buy_max_cost: bestYesPrice,
        client_order_id: `test-real-${Date.now()}`,
      };
      console.log('Request body:', JSON.stringify(realBody, null, 2));

      const realResult = await kalshi._post('/trade-api/v2/portfolio/orders', realBody);
      console.log('\n=== RAW REAL MARKET RESPONSE ===');
      console.log(JSON.stringify(realResult, null, 2));
    } else {
      console.log(`\nSkipping real fill test — best yes ask is ${bestYesPrice}¢ (too expensive or no book)`);
      console.log('To test a real fill, manually run with a specific ticker and price.');
    }

  } catch (e) {
    console.error('Error:', e.response?.data || e.message);
  }

  process.exit(0);
}

main().catch(e => { console.error(e); process.exit(1); });
