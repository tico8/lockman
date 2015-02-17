# lockman

## Description

Distributed lock with Redis

## Development

### Example
```javascript
  var mutex = require('lockman');
  var RedisSentinel = require('redis-sentinel-client');
  
  var option = {};
  option.redis.clients = [ RedisSentinel.createClient('localhost', 6379) ];
  mutex.setup(option);

  mutex.lock('user', '1', function(err, unlock) {
    if (err) {
      console.log('lock NG');
      return;
    }
    
    // your code
    console.log('lock OK');
    
    unlock(function(err) {
      if (err) {
        console.log('unlock NG');
        return;
      }
      console.log('unlock OK');
    });
  });
  
  // other pattarn
  mutex.lock('user', function(err, unlock) {}); // not use subKey
  mutex.lock('user', ['1', '2', '3'], function(err, unlock) {}); // subKey is Array
  mutex.lock('user', '1', { retry: 10, interval: 100 }, function(err, unlock) {}); // add option
  mutex.lock('user', { retry: 10, interval: 100 }, function(err, unlock) {}); // option only
```

### Options
