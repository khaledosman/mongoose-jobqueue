# mongoose-jobqueue

[![Greenkeeper badge](https://badges.greenkeeper.io/khaledosman/mongoose-jobqueue.svg)](https://greenkeeper.io/)

A modernized fork https://www.npmjs.com/package/mongoose-jobqueue that is compatible with mongoose v5 and the latest mongodb driver.

- Uses ES6 Promise instead of Bluebird
- Uses object spread instead of underscore/lodash dependency
- Compatible with mongoose v5
- Uses arrow functions instead of `var self = this;`
- Creates Mongodb indexes for faster access and query performance

# mongoose-jobqueue

A very simple job queue, using [mongoosejs](http://mongoosejs.com) for storage.

## Quickstart

Create a queue object by handing over your mongoose instance:

```js
var mongoose          = require('mongoose');
var mongooseJobQueue  = require('mongoose-jobqueue');

var queue = mongooseJobQueue(mongoose, 'job-queue');
```

All functions of the queue return a `Promise`.

Add a job to a queue:

```js
queue.add({ message: 'Hey' }).then(function(job){
  // Job with payload object added.
  // The created job-object is returned.
}, function(err){
  // Something went wrong!
});
```

Get a job from the queue:

```js
queue.checkout().then(function(job) {
  // Returns the next job in the queue, or null if queue is empty.
  console.log('job._id=' + job._id);
  console.log('job.ack=' + job.ack);          // Acknowledge key, save for later
  console.log('job.payload=' + job.payload);  // { message: 'Hey' }
  console.log('job.tries=' + job.tries);   
});
```

Ping a job to keep it's visibility open for long-running tasks:

```js
queue.ping(job.ack).then(function(job) {
  // Visibility window now increased for this job.
  // Updated job object is returned.
})
```

When pinging a job we can specify by how many seconds the window is extended,
and the progress of the job in percent if we want:

```js
queue.ping(job.ack, 60, 15).then(function(job) {
  // Visibility window now increased by 60 seconds. Job is 15% done.
  // Updated job object is returned.
})
```

Acknowledge a job (and remove it from the queue):

```js
queue.ack(job.ack).then(function(job) {
  // This job removed from queue for this ack.
  // The acknowledged job is returned.
})
```

By default, all finished jobs are left in the queue, and are only marked as
deleted. You can call the following function to remove processed jobs:

```js
queue.cleanup().then(function(delCount) {
    // All processed (ie. acked) messages have been deleted
    // The number of deleted jobs is returned.
});
```


## In-Code-Docs
The source code is documented using JSDoc tags.

## Creating a Queue ##

To create a queue, call the exported function with the Mongoose instance,
the name of the collection and a set of options.

```js
var mongoose          = require('mongoose');
var mongooseJobQueue  = require('mongoose-jobqueue');

// an instance of a queue
var queueA = mongooseJobQueue(mongoose, 'a-queue');
// another queue
var queueB = mongoDbQueue(mongoose, 'b-queue');
```

Note: You can start using the queue right away, since mongoose stores requests
until a connection to the MongoDB is established.

To pass in options for the queue:

```js
var myQueue = mongooseJobQueue(mongoose, 'my-queue', {
    visibility : 30,
    delay : 15
  });
```

This example shows a queue with a job visibility of 30s and a insert-delay
of 15s.

## Options

### name - Collection Name

This is the name of the MongoDB Collection you wish to use to store the jobs.
Each queue you create will be it's own collection.

e.g.

```js
var queueA = mongooseJobQueue(mongoose, 'a-queue');
var queueB = mongooseJobQueue(mongoose, 'b-queue');
```

This will create two collections in MongoDB called `a-queue` and `b-queue`.

### visibility - Job Visibility Window

Default: `30`

By default, if you don't ack a job within the first 30s after checking it out
of the queue, it is placed back in the queue so it can be fetched again.
This is called the visibility window.

You may set this visibility window on a per queue basis. For example, to set the
visibility to 15 seconds:

```js
var queue = mongooseJobQueue(mongoose, 'queue', { visibility : 15 });
```

All jobs in this queue now have a visibility window of 15s, instead of the
default 30s.

You can also specify the visibility window when checking out a job of the queue:

```js
var queue = mongooseJobQueue(mongoose, 'queue', { visibility : 15 });

queue.checkout(90).then(function(job){
  // Process the job...
});
```

The returned job now has a visibility window of 90s. This does not change the
visibility window of the queue or other jobs in the queue.

### delay - Delay Jobs on Queue

Default: `0`

When a job is added to a queue, it is immediately available for checkout.
However, there are times when you might like to delay jobs coming off a queue.
If you set delay to be `10`, then every job will only be available for
checkout 10s after being added.

To delay all jobs by 10 seconds, do this:

```js
var queue = mongooseJobQueue(mongoose, 'queue', { delay : 10 });
```

This is now the default for every job added to the queue.

### deadQueue - Dead Job Queue

Default: `null`

Jobs that have been retried over `maxRetries` will be pushed to this queue so
you can debug problematic jobs.

Pass in a collection name onto which these jobs will be pushed:

```js
var queue = mongooseJobQueue(mongoose, 'queue', { deadQueue : 'dead-jobs' });
```

If you checkout a job out of the `queue` over `maxRetries` times and have still
not acked it, it will be pushed onto the `deadQueue` for you.
This happens when you call `.checkout()` the next time. (not when you miss
acking a job within it's visibility window).

### maxRetries - Maximum Retries per Job

Default: `5`

This option only comes into effect if you pass in a `deadQueue` as shown above.
What this means is that if job is checked out of the queue `maxRetries` times
(e.g. 5) and not acked, it will be moved to the `deadQueue` the next time it is
checked out.

### strictAck - Disallow ack if Visibility Window timed out

Default: `true`

By default you are only allowed to acknowledge a checked out job within the
visibility window. Set this option to `false` if you want to allow acks of a job
outside of the visibility window, given that the job was not already checked out
a second time by another user.

### raw - Return raw JavaScript Objects

Default: `true`

By default all functions return plain JavaScript objects. Set this option to
`false` if you want the original mongoose documents returned. For example, this
gives you the ability to edit a returned job and call the `.save()` method on in.
Use this option at own risk.

### cosmosDb - Use compatibility mode for Azure CosmosDB

Default: `false`

CosmosDB does not support sorting on a findAndUpdate operation. (as of 2017-08-22)
Set this option to `true` to get unsorted results.

## Operations

### .add()

You can add a string to the queue:

```js
queue.add('Hey').then(function(job) {
  // Job with payload 'Hey' added.
  // Created job object is returned.
});
```

Or add an object:

```js
queue.add({ message: 'Hey' }).then(function(job) {
  // Job with payload { message: 'Hey' } added.
  // Created job object is returned.
});
```

Or add multiple jobs (strings or objects):

```js
queue.add(['One', 'Two', 'Three']).then(function(jobs) {
  // Jobs with payloads 'One', 'Two' & 'Three' added.
  // An array of the created job objects is returned.
});
```

You can delay jobs from being visible by passing the second `delay` parameter:

```js
queue.add('Later', 120).then(function(job) {
  // Job with payload 'Later' added.
  // Created job object is returned.
  // This job won't be available for checkout for 120 seconds.
});
```

### .checkout()

Retrieve a job from the queue:

```js
queue.checkout().then(function(job) {
  // You can now process the job
  // IMPORTANT: The message will be null if the queue is empty.
});
```

You can choose the visibility window of an individual retrieved job by passing
the `visibility` parameter:

```js
queue.checkout(90).then(function(job) {
  // You can now process the job for 90s before it goes back into the queue.
});
```

Jobs will have the following structure:

```js
{
  _id: '533b1eb64ee78a57664cc76c', // ID of the message
  ack: 'c8a3cc585cbaaacf549d746d7db72f69', // Key for ack and ping operations
  payload: 'Hey', // Payload passed when the job was addded
  tries: 1 // Number of times this job has been retrieved from queue without being ack'd
}
```

### .ack()

After you have received a job from a queue and processed it, you can delete it
by calling `.ack()` with the unique `ack` key returned:

```js
queue.checkout().then(function(job) {
  // Do some processing...

  queue.ack(job.ack).then(function(job) {
      // this job has now been removed from the queue
  });
});
```

### .ping()

After you have checked out a job from a queue and you are taking a while
to process it, you can `.ping()` the job to tell the queue that you are
still alive and continuing to process the job:

```js
queue.checkout().then(function(job) {
  queue.ping(job.ack).then(function(job) {
    // this job has had it's visibility window extended
  });
});
```

You can also choose the visibility time that gets added by the ping operation by
passing the `visibility` parameter:

```js
queue.checkout().then(function(job) {
  queue.ping(job.ack, 120).then(function(job) {
    // this job has had it's visibility window extended by 120 seconds
  });
});
```

You can pass an optional `progress` parameter, when pinging a job to show the
progress of your operation in percent:

```js
queue.checkout().then(function(job) {
  queue.ping(job.ack, 120, 12).then(function(job) {
    // this job has had it's visibility window extended by 120 seconds
    // The operation is completed by 12%
  });
});
```

Like the `progress` parameter you can pass the `payload` parameter to update the
payload of a job. This will overwrite the old payload value.

```js
queue.checkout().then(function(job) {
  queue.ping(job.ack, null, null, { message: 'my new payload' }).then(function(job) {
    // Updated job object is returned.
  });
});
```

### .get()

Get a list of jobs in the queue. Does not check out any jobs.

```js
queue.get().then(function(jobs){
  // Returns an array of all jobs in queue, processed or not
});
```

You can pass a filter object to only retrieve a subset of jobs.
The filter object follows the MongoDB query syntax.

```js
queue.get({
  deleted: null
}).then(function(jobs){
  // Returns an array of all that are not processed
});
```
This example returns only unfinished jobs.

```js
queue.get({
  tries: { $gt: 1, $lt: 4 }
}).then(function(jobs){

});
```
This example returns jobs with 2-3 tries.

```js
queue.get({
  'payload.message': 'Hey'
}).then(function(jobs){

});
```
This example returns jobs where the payload contains the message `Hey`.

### .cleanup()

By default, all finished jobs are left in the queue, and are only marked as
deleted. You can call the `cleanup()` function to remove processed jobs:

```js
queue.cleanup().then(function(delCount) {
  // All processed (ie. acked) jobs have been deleted.
  // The number of deleted jobs is returned.
});
```

You can specify the minimum age of jobs to be deleted by using the `age`
parameter:

```js
queue.cleanup(120).then(function(delCount) {
  // All processed (ie. acked) jobs older than 120 seconds have been deleted.
  // The number of deleted jobs is returned.
});
```

### .cleanupDead()

Removes all jobs from the deadQueue.
Unlike the `cleanup()` function, this operation does not have a age parameter.

```js
queue.cleanupDead().then(function(delCount) {
  // All dead jobs have been deleted.
  // The number of deleted jobs is returned.
});
```

### .reset()

Deletes ALL jobs from the queue (and the deadQueue if configured), regardless of
checked out jobs.

```js
queue.reset().then(function(totalDeleted) {
  // Queues are now empty, number of deleted jobs on both queues is returned.
});
```

## Author
Originally Written by [Michael Sperk](https://github.com/sperkm)
Updated by [Khaled Osman](https://github.com/khaledosman)

## License
MIT - https://choosealicense.com/licenses/mit/
