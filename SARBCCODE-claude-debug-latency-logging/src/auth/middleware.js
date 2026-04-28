/**
 * JWT authentication middleware.
 */

const jwt = require('jsonwebtoken');
const config = require('../config');

const JWT_SECRET = config.dashboard.secret || 'sarbccode-jwt-secret-change-me';
const JWT_EXPIRY = '7d';

/**
 * Generate a JWT token for a user.
 */
function signToken(username) {
  return jwt.sign({ username }, JWT_SECRET, { expiresIn: JWT_EXPIRY });
}

/**
 * Express middleware — verifies JWT from Authorization header.
 * Sets req.user = { username } on success.
 */
function requireAuth(req, res, next) {
  const header = req.headers.authorization || '';
  const token = header.startsWith('Bearer ') ? header.slice(7) : '';

  if (!token) {
    return res.status(401).json({ error: 'Authentication required' });
  }

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    req.user = { username: decoded.username };
    next();
  } catch {
    return res.status(401).json({ error: 'Invalid or expired token' });
  }
}

module.exports = { signToken, requireAuth };
