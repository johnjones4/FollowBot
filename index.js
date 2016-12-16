const kue = require('kue');
const request = require('request');
const querystring = require('querystring');
const config = require('./config');
const blacklist = require('./blacklist');

const JobName = 'followbot';
const Delay = 60000;
const JobTypes = {
  'SEARCH': 'search',
  'LISTFRIENDS': 'listFriends',
  'FOLLOW': 'follow',
}
const URLBase = 'https://api.twitter.com/1.1/';

var lastJobTime = 0;
var users = config.users;

const queue = kue.createQueue({
  'redis': {
    'port': (process.env.REDIS_PORT || 6379),
    'host': (process.env.REDIS_HOST || 'localhost'),
  }
});
kue.app.listen(process.env.KUE_PORT || 3000);

for(var userId in users) {
  users[userId].lastJobTime = 0;
  queue.create(JobName, {
    'title': 'Search for ' + userId,
    'type': JobTypes.SEARCH,
    'user': userId
  }).attempts(5).save();
}

process.once('SIGTERM',function ( sig ) {
  queue.shutdown(5000,function(err) {
    if (err) {
      console.error(err);
    }
    process.exit(0);
  });
});

queue.on('error',function(err) {
  console.error(err);
});

queue.process(JobName, function(job,done){
  var domain = require('domain').create();
  domain.on('error',function(err){
    done(err);
  });
  domain.run(function(){
    var now = new Date().getTime();
    if (now - users[job.data.user].lastJobTime > Delay) {
      processJob(job,done);
    } else {
      var backoff = ((Math.random() * (Delay / 2)) + Delay) - (now - users[job.data.user].lastJobTime);
      console.log('Delaying job by ' + backoff)
      setTimeout(function() {
        processJob(job,done);
      },backoff);
    }
  });
});

function processJob(job,done) {
  console.log('Processing job ' + job.data.type + ' job for ' + job.data.user);
  users[job.data.user].lastJobTime = new Date().getTime();
  switch(job.data.type) {
    case JobTypes.SEARCH:
      var params = {
        'q': users[job.data.user].search,
        'count': 100
      };
      if (job.data.since_id) {
        params.since_id = job.data.since_id;
      } else if (job.data.max_id) {
        params.max_id = job.data.max_id;
      }
      request.get({
        'url': URLBase + 'search/tweets.json?' + querystring.stringify(params),
        'oauth': users[job.data.user].oauth,
        'json': true
      },function(err, r, body) {
        if (err) {
          done(err);
        } else if (body.errors) {
          done(new Error(JSON.stringify(body.errors)));
        } else {
          console.log(body);
          var usersToFollow = body.statuses.map(function(status) {
            return status.user;
          }).filter(function(user) {
            if (!users[job.data.user].following) {
              users[job.data.user].following = [];
            }
            if (users[job.data.user].following.indexOf(user.id) < 0 && blacklist[job.data.user].indexOf(user.id) < 0) {
              users[job.data.user].following.push(user.id);
              return true;
            } else {
              return false;
            }
          });
          usersToFollow.forEach(function(user) {
            queue.create(JobName, {
              'title': 'Follow ' + user.screen_name,
              'type': JobTypes.FOLLOW,
              'user': job.data.user,
              'follow': user.id
            }).attempts(5).save();
          });
          queue.create(JobName, {
            'title': 'Find friends for ' + job.data.user,
            'type': JobTypes.LISTFRIENDS,
            'user': job.data.user
          }).attempts(5).save();
          if (body.statuses.length > 0 && body.search_metadata && body.search_metadata.max_id && body.search_metadata.max_id > 0) {
            queue.create(JobName, {
              'title': 'Search for ' + job.data.user + ' (Previous ' + body.search_metadata.max_id + ')',
              'type': JobTypes.SEARCH,
              'user': job.data.user,
              'max_id': body.search_metadata.max_id
            }).attempts(5).save();
          }
          if (body.statuses.length == 0) {
            queue.create(JobName, {
              'title': 'Search for ' + job.data.user + ' (No new results)',
              'type': JobTypes.SEARCH,
              'user': job.data.user
            }).attempts(5).save();
          } else {
            queue.create(JobName, {
              'title': 'Search for ' + job.data.user + ' (Next ' + body.statuses[0].id + ')',
              'type': JobTypes.SEARCH,
              'user': job.data.user,
              'since_id': body.statuses[0].id
            }).attempts(5).save();
          }
          done();
        }
      });
      break;
    case JobTypes.LISTFRIENDS:
      var params = {
        'user_id': job.data.user,
        'count': 5000
      };
      if (job.data.cursor) {
        params.cursor = job.data.cursor;
      }
      request.get({
        'url': URLBase + 'friends/ids.json?' + querystring.stringify(params),
        'oauth': users[job.data.user].oauth,
        'json': true
      },function(err, r, body) {
        if (err) {
          done(err);
        } else if (body.errors) {
          done(new Error(JSON.stringify(body.errors)));
        } else {
          console.log(body);
          if (!users[job.data.user].following) {
            users[job.data.user].following = [];
          }
          body.ids.forEach(function(id) {
            if (users[job.data.user].following.indexOf(id) < 0) {
              users[job.data.user].following.push(id);
            }
          });
          if (body.next_cursor && body.next_cursor > 0) {
            queue.create(JobName, {
              'title': 'Find friends for ' + job.data.user + ' (' + body.next_cursor + ')',
              'type': JobTypes.LISTFRIENDS,
              'user': job.data.user,
              'cursor': body.next_cursor
            }).attempts(5).save();
          }
          done();
        }
      });
      break;
    case JobTypes.FOLLOW:
      request.post({
        'url': URLBase + 'friendships/create.json?' + querystring.stringify({'user_id':job.data.follow,'follow':true}),
        'oauth': users[job.data.user].oauth,
        'json': true
      },function(err, r, body) {
        if (err) {
          done(err);
        } else {
          console.log(body);
          if (body.errors) {
            done(new Error(JSON.stringify(body.errors)));
          } else {
            done();
          }
        }
      });
      break;
  }
}
