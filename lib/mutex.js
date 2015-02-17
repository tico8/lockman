/**
 * @fileoverview Distributed locks with Redis
 * @author furuya_kaoru
 */

var crypto = require('crypto');
var RedisSentinel = require('redis-sentinel-client');

// const
var MAX_STORES = 256;
var KEY_SEPARATOR = '-';
var STORE_STATUS_UNINIT = 'uninit';
var STORE_STATUS_ACTIVE = 'active';
var STORE_STATUS_DOWN = 'down';
var STORE_STATUS_FAILOVER = 'failover';
var STORE_STATUS_UNKNOWN = 'unknown';

var DEFAULT_REQEST_RETRY = null; // no limit
var DEFAULT_REQEST_INTERVAL = 100; // 100 msec

var DEFAULT_LOCK_EXPIRY = 10000; // 10 sec
var DEFAULT_LOCK_KEY_PREFIX = 'LOCKMAN#';
var DEFAULT_LOCK_VALUE_PREFIX = null;
var DEFAULT_LOCK_VALUE_LENGTH = 12; // Accuracy of a random value

/**
 * Distributed locks
 * @constructor
 */
function Mutex() {
  this.option = {
    'retry': DEFAULT_REQEST_RETRY,
    'interval': DEFAULT_REQEST_INTERVAL,
    'expiry': DEFAULT_LOCK_EXPIRY,
    'expiryOfKey': {},
    'keyPrefix': DEFAULT_LOCK_KEY_PREFIX,
    'valuePrefix': DEFAULT_LOCK_VALUE_PREFIX,
    'valueLength': DEFAULT_LOCK_VALUE_LENGTH,
    'redis': {
      'sclients': [
        {
          'masterName': 'mymaster',
          'sentinels': [
            ['localhost', 26379]
          ]
        }
      ]
    }
  };
  this.stores = [];
  this.storeStatus = [];
  this.logger = {
    debug: console.log,
    info: console.info,
    warn: console.warn,
    error: console.error
  };
}

/**
 * Create lock key
 * @param {string} key - main key
 * @param {string} subKey - sub key (not require)
 * @return {string} key of lock
 */
Mutex.prototype._createLockKey = function(key, subKey) {
  var lockKey = this.option.keyPrefix + key;
  if (subKey) {
    lockKey = lockKey + KEY_SEPARATOR + subKey;
  }
  return lockKey;
};

/**
 * Create lock value
 * @return {string} value - value of lock
 */
Mutex.prototype._createLockValue = function() {
  var len = this.option.valueLength;
  var value = crypto.randomBytes(Math.ceil(len / 2))
    .toString('hex').slice(0, len);
  if (this.option.valuePrefix) {
    value = this.option.valuePrefix + value;
  }

  return value;
};

/**
 * Get index of Redis which sets a lock key
 * @param {string} lockKey - key of lock
 */
Mutex.prototype._getStoreIndex = function(lockKey) {
  if (this.stores.length <= 0) {
    return 0;
  }

  var hash = crypto.createHash('md5').update(lockKey).digest('hex');
  var value = parseInt(hash.substring(hash.length - 2), 16); // 0 ~ 255
  return value % this.stores.length;
};

/**
 * Get time which blocks lock acquisition
 * @param {string} key - main key
 * @param {string} subKey - sub key
 * @return {number} Milli second
 */
Mutex.prototype._getBlockingTime = function(key, subKey) {
  var lockKey = this._createLockKey(key, subKey);
  var storeIndex = this._getStoreIndex(lockKey);
  var status = this.storeStatus[storeIndex];
  if (!status) {
    return 0;
  }

  var now = Date.now();
  var expiry = this.option.expiryOfKey[key] || this.option.expiry;
  if (now >= status.latestUnusualTime + expiry) {
    return 0;
  }

  return status.latestUnusualTime + expiry - now;
};

/**
 * Get status of Redis
 * @param {number} storeIndex - index of Redis
 * @return {string}
 */
Mutex.prototype._getStoreStatus = function(storeIndex) {
  var status = this.storeStatus[storeIndex];
  if (!status) {
    return STORE_STATUS_UNINIT;
  }

  return status.code;
};

/**
 * Set status of Redis
 * @param {number} storeIndex - index of Redis
 * @param {string} statusCode - status of Redis
 */
Mutex.prototype._setStoreStatus = function(storeIndex, statusCode) {
  var now = Date.now();
  var status = this.storeStatus[storeIndex];
  if (!status) {
    status = {
      'code': statusCode,
      'latestUnusualTime': 0
    };
  }

  switch (statusCode) {
    case STORE_STATUS_ACTIVE:
      break;
    case STORE_STATUS_DOWN:
    case STORE_STATUS_FAILOVER:
      status.latestUnusualTime = Math.max(status.latestUnusualTime, now);
      break;
    default:
      status.statusCode = STORE_STATUS_UNKNOWN;
      break;
  }

  this.storeStatus[storeIndex] = status;
};

/**
 * Wait status of all Stores is active
 * @param {Function} callback - コールバック
 * @this
 */
Mutex.prototype._waitStoreActive = function(callback) {
  var self = this;
  var check = function(callback) {
    for (var i = 0; i < self.stores.length; i++) {
      if (self._getStoreStatus(i) !== STORE_STATUS_ACTIVE) {
        self.logger.debug('[ Redis_' + i + ' ] wait connection');
        return setTimeout(check, 100, callback);
      }
    }
    self.logger.info('[ Redis_ALL ] connected');
    return callback(null);
  };
  check(callback);
};

/**
 * Setup Redis Clients
 * @param {Function} callback
 */
Mutex.prototype._setupStore = function(callback) {
  if (!this.option.redis) {
    return callback && callback(new Error('option.redis is not found.'));
  }

  var type = null;
  if (this.option.redis.clients) {
    type = 'clients';
  } else if (this.option.redis.sclients) {
    type = 'sclients';
  } else {
    return callback && callback(new Error('option.redis is invalid.'));
  }

  if (this.option.redis[type].length <= 0) {
    return callback && callback(new Error('option.redis.' + type + ' is 0.'));
  }
  if (this.option.redis[type].length > MAX_STORES) {
    return callback && callback(new Error('option.redis.' + type + ' is too many. max = ' + MAX_STORES));
  }

  if (type === 'clients') {
    // outer client
    for (var i = 0; i < this.option.redis.clients.length; i++) {
      var client = this.option.redis.clients[i];
      this._addStore(i, client);
      return callback && callback();
    }
  } else if (type === 'sclients') {
    // inner client
    for (var j = 0; j < this.option.redis.sclients.length; j++) {
      var sentinelConf = this.option.redis.sclients[j];
      this._addStore(j, this._createStore(sentinelConf.sentinels, sentinelConf.masterName, this.option.redis.masterOption)); // sclients use same masterOption.
    }
    this._waitStoreActive(callback);
  }
};

/**
 * Create RedisClient
 * @param {Array} sentinels - redis sentinel host and port list. [['host', port], ...]
 * @param {string} masterName - redis master name
 * @param {Object} masterOption - redis master option(node_redis)
 * @return {RedisClient}
 */
Mutex.prototype._createStore = function(sentinels, masterName, masterOption) {
  return RedisSentinel.createClient({
    'sentinels': sentinels,
    'masterName': masterName,
    'masterOptions': masterOption || {}
  });
};

/**
 * Add RedisClient and setup.
 * @param {number} storeIndex - index of Redis
 * @param {RedisClient} redisClient - redis client
 */
Mutex.prototype._addStore = function(storeIndex, redisClient) {
  var self = this;

  if (!this.stores[storeIndex]) {
    this.stores[storeIndex] = redisClient;

    this.stores[storeIndex].on('connect', function() {
      self.logger.debug('[ Redis_' + storeIndex + ' ] connect');
      self._setStoreStatus(storeIndex, STORE_STATUS_ACTIVE);
    });
    this.stores[storeIndex].on('end', function() {
      self.logger.error('[ Redis_' + storeIndex + ' ] end');
      self._setStoreStatus(storeIndex, STORE_STATUS_DOWN);
    });
    this.stores[storeIndex].on('error', function(err) {
      self.logger.error('[ Redis_' + storeIndex + ' ] error : ' + err);
    });
    this.stores[storeIndex].on('sentinel connect', function() {
      self.logger.debug('[ RedisSn_' + storeIndex + ' ] sentinel connect');
    });
    this.stores[storeIndex].on('sentinel connected', function() {
      self.logger.debug('[ RedisSn_' + storeIndex + ' ] sentinel connected');
    });
    this.stores[storeIndex].on('sentinel disconnected', function() {
      self.logger.error('[ RedisSn_' + storeIndex + ' ] sentinel disconnected');
    });
    this.stores[storeIndex].on('sentinel message', function(msg) {
      self.logger.warn('[ RedisSn_' + storeIndex + ' ] sentinel message : ' + msg);
    });
    this.stores[storeIndex].on('failover start', function() {
      self.logger.error('[ RedisSn_' + storeIndex + ' ] failover start');
      self._setStoreStatus(storeIndex, STORE_STATUS_FAILOVER);
    });
    this.stores[storeIndex].on('failover end', function() {
      self.logger.error('[ RedisSn_' + storeIndex + ' ] failover end');
    });
    this.stores[storeIndex].on('switch master', function() {
      self.logger.error('[ RedisSn_' + storeIndex + ' ] switch master');
    });
  }
};

/**
 * Setup Mutex
 * @param {Object} option
 * @param {Function} callback
 */
Mutex.prototype.setup = function(option, callback) {
  if (!option) {
    return callback(new Error('option is not found.'));
  }

  this.option.retry = option.retry || this.option.retry;
  this.option.interval = option.interval || this.option.interval;
  this.option.expiry = option.expiry || this.option.expiry;
  this.option.expiryOfKey = option.expiryOfKey || this.option.expiryOfKey;
  this.option.keyPrefix = option.keyPrefix || this.option.keyPrefix;
  this.option.valuePrefix = option.valuePrefix || this.option.valuePrefix;
  this.option.valueLength = option.valueLength || this.option.valueLength;

  // logger
  this.logger = option.logger || this.logger;

  // redis
  this.option.redis = option.redis;
  this._setupStore(callback);
};

/**
 * acquire lock
 * @param {string} key - main key
 * @param {string} subKey - sub key (not require)
 * @param {Object} option - (not require)
 * @param {Function} callback
 */
Mutex.prototype.lock = function(key, subKey, option, callback) {
  // variable argument
  switch (arguments.length) {
    case 4:
      break;
    case 3:
      if (typeof option === 'function') {
        callback = option;
        option = {};
      }
      if (typeof subKey === 'string' || subKey instanceof String || subKey instanceof Array ||
          typeof subKey === 'number' || subKey instanceof Number) {
        subKey = subKey;
        option = {};
      } else if (typeof subKey === 'object') {
        option = subKey;
        subKey = null;
      } else {
        return callback(new Error('Unsupported arguments'), null);
      }
      break;
    case 2:
      if (typeof subKey === 'function') {
        callback = subKey;
        subKey = null;
        option = {};
      }
      break;
    default:
      return callback(new Error('Unsupported arguments'), null);
  }
  if (subKey instanceof Array) {
    subKey = subKey.join(KEY_SEPARATOR);
  }

  var self = this;
  var lockKey = this._createLockKey(key, subKey);
  var lockValue = this._createLockValue();
  var storeIndex = this._getStoreIndex(lockKey);
  var retry = option.retry || this.option.retry;
  var interval = option.interval || this.option.interval;
  var expiry = this.option.expiryOfKey[key] || this.option.expiry;

  // unlock function
  var unlock = function(cb) {
    self._unlock(lockKey, lockValue, cb);
  };

  // retry function
  var retryCount = 0;
  var fn = function() {
    if (retry && retry < retryCount) {
      return callback(new Error('Lock acquisition is retry failure. : storeIndex = ' + storeIndex +
        ' lockKey = ' + lockKey + ' retry = ' + retry + ' interval = ' + interval), null);
    }
    if (!self.stores[storeIndex] || self._getStoreStatus(storeIndex) === STORE_STATUS_UNINIT) {
      return callback(new Error('Store is uninitialized. : storeIndex = ' + storeIndex), null);
    }
    var blockingTime = self._getBlockingTime(key, subKey);
    if (blockingTime > 0) {
      return callback(new Error('Lock acquisition is blocked. : storeIndex = ' + storeIndex +
        ' blockingTime = ' + blockingTime + ' lockKey = ' + lockKey), null);
    }

    self.stores[storeIndex].send_command('set', [lockKey, lockValue, 'PX', expiry, 'NX'],
      function(err, result) {
        if (err) {
          return callback(err, null);
        }

        // lock ok
        if (result && result === 'OK') {
          self.logger.debug('lock key = ' + lockKey + ' value = ' + lockValue +
              ' storeIndex = ' + storeIndex + ' expiry = ' + expiry);
          return callback(null, unlock);
        }

        // retry
        retryCount++;
        self.logger.debug('retry key = ' + lockKey + ' value = ' + lockValue +
            ' storeIndex = ' + storeIndex + ' retry = ' + retry + ' interval = ' + interval);
        return setTimeout(fn, interval);
      }
    );
  };

  fn();
};

/**
 * unlock
 * @param {string} lockKey - key of lock
 * @param {string} lockValue - value of lock
 * @param {Function} callback
 */
Mutex.prototype._unlock = function(lockKey, lockValue, callback) {
  var self = this;
  var storeIndex = this._getStoreIndex(lockKey);

  // redis lua script
  var script =
    'if redis.call("get", KEYS[1]) == ARGV[1] then' + '\n' +
    '    return redis.call("del", KEYS[1])' + '\n' +
    'else' + '\n' +
    '    return 0' + '\n' +
    'end';
  this.stores[storeIndex].send_command('eval', [script, 1, lockKey, lockValue], function(err, result) {
    if (err) {
      return callback && callback(err);
    }

    var alreadyUnlocked = false;
    if (result === 0) {
      alreadyUnlocked = true;
      self.logger.warn('already unlocked key = ' + lockKey + ' value = ' + lockValue + ' storeIndex = ' + storeIndex);
    } else {
      self.logger.debug('unlock key = ' + lockKey + ' value = ' + lockValue + ' storeIndex = ' + storeIndex);
    }
    return callback && callback(null, alreadyUnlocked);
  });
};

module.exports = new Mutex();
