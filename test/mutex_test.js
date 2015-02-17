var assert = require('assert');
var RedisSentinel = require('redis-sentinel-client');

var mutex = require('../lib/mutex');

var EXPIRY = 3000;
var EXPIRY_OF_KEY = 6000;
var SENTINEL_CONF = {
  'sentinels': [
    ['localhost', 26379]
  ],
  'masterName': 'master'
};

before(function(done) {
  console.log('[describe]before test');

  // setup
  var option = {
    'retry': null,
    'interval': 100,
    'expiry': EXPIRY,
    'expiryOfKey': {
      'longExpiryKey': EXPIRY_OF_KEY
    },
    'keyPrefix': 'KPREFIX#',
    'valuePrefix': 'VPREFIX#',
    'valueLength': 24,
    'redis': {
      // alternative is clients or sclients
      'clients': [RedisSentinel.createClient(SENTINEL_CONF), RedisSentinel.createClient(SENTINEL_CONF)]
//      'sclients': [SENTINEL_CONF, SENTINEL_CONF]
    }
  };
  mutex.setup(option, function(err) {
    if (err) {
      console.log(err);
    }
    done();
  });
});

after(function(done) {
  console.log('[describe]after test');
  done();
});

beforeEach(function(done) {
  console.log('[it]before every test');
  done();
});

afterEach(function(done) {
  console.log('[it]after every test');
  done();
});

describe('mutex', function() {
  describe('lock', function() {
    it('lock,unlockできること', function(done) {
      mutex.lock('testkey1', function(err, unlock) {
        assert.equal(err, null);

        unlock(function(err, alreadyUnlocked) {
          assert.equal(err, null);

          done();
        });
      });
    });

    it('lock,unlockできること', function(done) {
      mutex.lock('testKey2', 'testSubKey', function(err, unlock) {
        assert.equal(err, null);

        unlock(function(err, alreadyUnlocked) {
          assert.equal(err, null);

          done();
        });
      });
    });

    it('lock,unlockできること', function(done) {
      mutex.lock('testKey3', ['testSubKey', '1', '2'], function(err, unlock) {
        assert.equal(err, null);

        unlock(function(err, alreadyUnlocked) {
          assert.equal(err, null);

          done();
        });
      });
    });

    it('lock,unlockできること', function(done) {
      mutex.lock('testKey4', 1, function(err, unlock) {
        assert.equal(err, null);

        unlock(function(err, alreadyUnlocked) {
          assert.equal(err, null);

          done();
        });
      });
    });

    it('lock,unlockできること', function(done) {
      mutex.lock('testKey5', ['testSubKey', 1, 2], function(err, unlock) {
        assert.equal(err, null);

        unlock(function(err, alreadyUnlocked) {
          assert.equal(err, null);

          done();
        });
      });
    });

    it('lock,unlockできること（オプションあり、サブキーあり）', function(done) {
      var opt = {
        'retry': 3,
        'interval': 10
      };
      mutex.lock('testkey', 'testSubKey', opt, function(err, unlock) {
        assert.equal(err, null);

        unlock(function(err, alreadyUnlocked) {
          assert.equal(err, null);

          done();
        });
      });
    });

    it('lock,unlockできること（オプションあり、サブキーなし）', function(done) {
      var opt = {
        'retry': 3,
        'interval': 10
      };
      mutex.lock('testkey', opt, function(err, unlock) {
        assert.equal(err, null);

        unlock(function(err, alreadyUnlocked) {
          assert.equal(err, null);

          done();
        });
      });
    });

    it('lockできないこと（リトライ回数オーバー）', function(done) {
      var opt = {
        'retry': 3,
        'interval': 10
      };
      mutex.lock('testkey', opt, function(err, unlock1) { // dead lock
        assert.equal(err, null);

        // aquire lock in dead lock
        mutex.lock('testkey', opt, function(err, unlock2) {
          assert.notEqual(err, null);
          console.log(err);
          assert.equal(unlock2, null);

          unlock1(function() {
            done();
          });
        });
      });
    });

    it('deadlockがexpirされること', function(done) {
      this.timeout(EXPIRY * 2);
      mutex.lock('testkey', function(err, unlock) {
        assert.equal(err, null);

        setTimeout(unlock, EXPIRY + 100, function(err, alreadyUnlocked) {
          assert.equal(err, null);
          assert.equal(alreadyUnlocked, true);

          done();
        });
      });
    });

    it('deadlockがexpirされること(特定のkeyのExpiryを長くした場合)', function(done) {
      this.timeout(EXPIRY_OF_KEY * 2);
      mutex.lock('longExpiryKey', function(err, unlock) {
        assert.equal(err, null);

        setTimeout(function() {
          unlock(function(err, alreadyUnlocked) {
            assert.equal(err, null);
            assert.equal(alreadyUnlocked, true);

            done();
          });
        }, EXPIRY_OF_KEY + 100);
      });
    });

    it('redisに異変があった場合、expiryの期間中はlockできないこと', function(done) {
      this.timeout(EXPIRY * 2);
      mutex._setStoreStatus(0, 'down');
      mutex._setStoreStatus(0, 'active');

      mutex.lock('testkey', function(err, unlock) {
        assert.notEqual(err, null);
        console.log(err);
        assert.equal(unlock, null);
      });

      setTimeout(function() {
        mutex.lock('testkey', function(err, unlock) {
          assert.equal(err, null);
          assert.notEqual(unlock, null);

          unlock(function() {
            done();
          });
        });
      }, EXPIRY + 100);
    });

    it('redisに異変があった場合、expiryの期間中はlockできないこと(特定のkeyのExpiryを長くした場合)', function(done) {
      this.timeout(EXPIRY_OF_KEY * 2);
      mutex._setStoreStatus(0, 'down');
      mutex._setStoreStatus(0, 'active');

      mutex.lock('longExpiryKey', function(err, unlock) {
        assert.notEqual(err, null);
        console.log(err);
        assert.equal(unlock, null);
      });

      setTimeout(function() {
        mutex.lock('longExpiryKey', function(err, unlock) {
          assert.equal(err, null);
          assert.notEqual(unlock, null);

          unlock(function() {
            done();
          });
        });
      }, EXPIRY_OF_KEY + 100);
    });

//    it('永遠とlock,unlockする。（障害時確認用）', function(done) {
//      this.timeout(10000000);
//      //キャッチされない例外処理を追加
//      mochaHandler = process.listeners('uncaughtException').pop();
//      process.on('uncaughtException', function(err) {
//        console.log(err);
//      });
//
//      var d = require('domain').create();
//      d.on('error', function (er) {
//        console.error('caught error:', er.message);
//      });
//
//      fn = function() {
//        try {
//          mutex.lock('recursive', function(err, unlock) {
//            if (err) {
//              console.log(err);
//              setTimeout(fn, 1000);
//            } else {
//              unlock(function(err) {
//                if (err) {
//                  console.log(err);
//                }
//                setTimeout(fn, 1000);
//              });
//            }
//          });
//        } catch (err) {
//          console.log(err);
//        }
//      };
//
//      fn();
//    });

  });
});
