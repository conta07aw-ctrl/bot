#!/usr/bin/env node
/**
 * CLI tool to manage invite codes.
 *
 * Usage:
 *   node scripts/invite.js create              — create lifetime invite code
 *   node scripts/invite.js create 30           — create invite that expires in 30 days
 *   node scripts/invite.js list                — list all invite codes
 */

require('dotenv').config();
const { createInvite, listInvites } = require('../src/store/users');

const [,, cmd, arg] = process.argv;

switch (cmd) {
  case 'create': {
    const days = arg ? parseInt(arg, 10) : null;
    const invite = createInvite(days);
    console.log('');
    console.log('  Invite code created!');
    console.log(`  Code:    ${invite.code}`);
    console.log(`  Expires: ${invite.expiresAt || 'NEVER (lifetime)'}`);
    console.log('');
    break;
  }

  case 'list': {
    const invites = listInvites();
    const entries = Object.entries(invites);
    if (entries.length === 0) {
      console.log('\n  No invite codes found.\n');
      break;
    }
    console.log('');
    console.log('  CODE             | STATUS   | EXPIRES            | USED BY');
    console.log('  -----------------+----------+--------------------+--------');
    for (const [code, inv] of entries) {
      const expired = inv.expiresAt && new Date(inv.expiresAt) < new Date();
      const status = inv.used ? 'USED' : expired ? 'EXPIRED' : 'ACTIVE';
      const expires = inv.expiresAt ? inv.expiresAt.slice(0, 10) : 'lifetime';
      const usedBy = inv.usedBy || '-';
      console.log(`  ${code} | ${status.padEnd(8)} | ${expires.padEnd(18)} | ${usedBy}`);
    }
    console.log('');
    break;
  }

  default:
    console.log('');
    console.log('  Usage:');
    console.log('    node scripts/invite.js create          — lifetime invite');
    console.log('    node scripts/invite.js create 30       — expires in 30 days');
    console.log('    node scripts/invite.js list            — list all codes');
    console.log('');
}
