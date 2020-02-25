/**
 * Mongoose JobQueue Module
 * module mongoose-jobqueue
 * description A simple jobqueue using [mongoosejs](http://mongoosejs.com).
 * This document follows the [JSDocs](http://usejsdoc.org/) markup.
 */

/**
 * @typedef {Object} Job
 * @property {string} id The unique id of the job.
 * @property {number} tries Number of times the job has been checked out.
 */

/**
 * A promise for a job id
 *
 * @promise JobIdPromise
 * @fulfill {string} The id of the job.
 * @reject {QueueEmptyError} The queue was empty.
 * @reject {Error} An unknown error occured.
 */

/**
 * Configuration Object for the JobQueue Class
 * @typedef {Object} JobQueueConfigurationObject
 * @property {string} [deadQueue=null] Collection name of the dead queue.
 * Dead queue is disabled if this is null.
 * @property {number} [delay=0] Default delay, until job becomes visible.
 * [seconds]
 * @property {number} [maxRetries=5] Maximum number of checkouts before a job
 * is pushed to the dead queue.
 * (only if a collection for the deadQueue is specified)
 * @property {boolean} [strictAck=true] Do not allow the acknowledgement of a
 * job whos visibility window has elapsed.
 * @property {number} [visibility=30] Default visibility time for jobs. [seconds]
 * @property {boolean} [raw=true] Return plain JavaScript objects instead of
 * mongoose documents.
 * @property {boolean} [cosmosDb=false] Azure CosmosDB Mode
 */

// Require Libraries
// -----------------------------------------------------------------------------
const crypto = require('crypto')
const mongooseLib = require('mongoose')

/**
 * JobQueueHelper Class
 * @description Contains helper functions for the JobQueue class.
 * @class
 */
class JobQueueHelper {
  /**
   * Build the mongoose model for the queue.
   *
   * @param {Mongoose} mongoose Mongoose instance.
   * @param {String} name Name of the model to be created.
   * @return {Mongoose.Model} New instance of a Mongoose Model.
   */
  static buildModel (mongoose, name) {
    const Schema = mongooseLib.Schema
    const schema = new Schema({
      payload: {
        type: Schema.Types.Mixed,
        required: true
      },
      visible: {
        type: Date,
        default: Date.now
      },
      tries: {
        type: Number,
        default: 0
      },
      ack: {
        type: String
      },
      deleted: {
        type: Date
      },
      progress: {
        type: Number
      }
    }, {
      collection: name
    })

    schema.index({ ack: 1, deleted: -1 })
    schema.index({ deleted: -1, visible: -1 })

    // Attach virtual properties
    schema.virtual('inFlight').get(() => {
      if (this.deleted) {
        return false
      }

      if (this.tries <= 0) {
        return false
      }

      const now = new Date()
      if (this.visible < now) {
        return false
      }

      return true
    })

    // Default options for conversion to objects
    // schema.set('toObject', { virtuals: true, versionKey: false });

    return mongoose.model(name, schema)
  }

  /**
   * Return a random hex string
   *
   * @return {String} Hex representation of random 16 bytes.
   */
  static id () {
    return crypto.randomBytes(16).toString('hex')
  }

  /**
   * Return a date object with the current time
   *
   * @return {Date} New instance of Date set to now.
   */
  static now () {
    return new Date()
  }

  /**
   * Return a date object with the current time plus a number of seconds.
   *
   * @param {number} secs Number of seconds to add to the current timestamp.
   * @return {Date} New instance of Date set to n seconds in the future.
   */
  static nowPlusSecs (secs) {
    return (new Date(Date.now() + secs * 1000))
  }

  /**
   * Prepare output.
   *
   * @param {(mongoose.Document | mongoose.Document[])} doc Mongoose document
   * or array of Mongoose documents
   * @param {boolean} raw Convert mongoose documents to plain objects
   * @return {(object | object[])} Object or array of objects
   */
  static prep (doc, raw) {
    if (!raw) {
      return doc
    }

    if (!doc) {
      return doc
    }

    if (!Array.isArray(doc)) {
      if (doc.toObject) {
        return doc.toObject({ getters: true, versionKey: false })
      }
      return doc
    }

    const docs = []

    for (const d of doc) {
      if (d.toObject) {
        docs.push(d.toObject({ getters: true, versionKey: false }))
      } else {
        docs.push(d)
      }
    }

    return docs
  }
}

/**
 * JobQueue Class
 * @class
 */
class JobQueue {
  /**
   * Constructor
   * @constructor
   * @param {Mongoose} mongoose Mongoose instance.
   * @param {String} name Name of the collection in the mongodb.
   * @param {JobQueueConfigurationObject} [opts={}] Configuration object.
   * @return {JobQueue} New instance of JobQueue.
   */
  constructor (mongoose, name, opts) {
    if (!mongoose) {
      throw new Error('mongoose-jobqueue: provide a mongoose instance')
    }
    if (!name) {
      throw new Error('mongoose-jobqueue: provide a queue name')
    }

    // Default options
    this.options = {
      delay: 0,
      visibility: 30,
      strictAck: true,
      maxRetries: 5,
      deadQueue: null,
      raw: true,
      cosmosDb: false
    }

    // Extend default options with the ones passed to the constructor
    // _.extendOwn(this.options, opts)
    opts = { ...opts, ...this.options }

    this.name = name
    this.mongoose = mongoose
    this.queue = JobQueueHelper.buildModel(this.mongoose, name)
    this.deadQueue = null

    // Init dead queue model if it is enabled
    if (this.options.deadQueue) {
      this.deadQueue = JobQueueHelper.buildModel(this.mongoose, this.options.deadQueue)
    }
  }

  /**
   * Add one ore more jobs to the queue
   *
   * @param {Object | Object[]} payload Payload object,
   * or array of payload Objects.
   * @param {number} [delay=JobQueue.delay] Timespan after which the job will
   * become visible in the queue. Overrides the options set at the construction
   * of the JobQueue instance. [seconds]
   * @return {Promise<Job[]>} Array containing the added jobs with their ids.
   */
  add (payload, delay) {
    delay = delay || this.options.delay

    return new Promise((resolve, reject) => {
      const inserts = []

      // Determine time at which the job will be visible in the queue
      const visible = delay ? JobQueueHelper.nowPlusSecs(delay) : JobQueueHelper.now()

      // Check if we have one or more jobs to add
      if (payload instanceof Array) {
        if (payload.length === 0) {
          reject(new Error('JobQueue.add(): Payload array length must be greater than 0.'))
          return
        }

        payload.forEach((payload) => {
          inserts.push({
            visible: visible,
            payload: payload
          })
        })
      } else {
        inserts.push({
          visible: visible,
          payload: payload
        })
      }

      this.queue.create(inserts)
        .then((result) => {
          if (result === null) {
            reject(new Error('Mongoose returned empty result on creation.'))
            return
          }

          if (inserts.length > 1) {
            resolve(JobQueueHelper.prep(result, this.options.raw))
            return
          }

          resolve(JobQueueHelper.prep(result[0], this.options.raw))
        }, (error) => {
          reject(new Error(error))
        })
    })
  }

  /**
   * Checkout a job from the queue
   *
   * @param {number} [visibility=JobQueue.visibility] Visibility window for the
   * checked out job. Overrides the global setting if set. [seconds]
   * @return {Promise<Job>} Job from the queue, or null if the queue was empty.
   */
  checkout (visibility) {
    visibility = visibility || this.options.visibility

    return new Promise((resolve, reject) => {
      const query = {
        deleted: null, // Only fetch jobs that are not finished
        visible: {
          $lte: JobQueueHelper.now() // Only fetch jobs that are visible yet
        }
      }

      const options = {
        sort: {
          _id: 1 // Sort by _id
        },
        new: true, // Return the updated document as result
        lean: true
      }

      // CosmosDB does not support sorting on update
      // if (this.options.cosmosDb) {
      //   options.sort = undefined
      // }

      const update = {
        $set: {
          ack: JobQueueHelper.id(),
          visible: JobQueueHelper.nowPlusSecs(visibility)
        },
        $inc: {
          tries: 1
        }
      }

      this.queue.findOneAndUpdate(query, update, options)
        .then((job) => {
        // Just resolve if result was empty, nothing more to do here.
          if (job === null) {
            resolve(null)
            return
          }

          // Check if we have a dead queue, if not, there is no need to check the
          // maxRetries.
          if (!this.deadQueue) {
            resolve(JobQueueHelper.prep(job, this.options.raw))
            return
          }

          // We are within the retry limit, no action required.
          if (job.tries <= this.options.maxRetries) {
            resolve(JobQueueHelper.prep(job, this.options.raw))
            return
          }

          // The retry limit has been exceeded, move to the deadQueue, acknowledge
          // in this queue and and try to return another job from the queue
          this.deadQueue.create({
            payload: job.payload,
            tries: job.tries
          })
            .then((deadJob) => {
              this.ack(job.ack)
                .then((ackJob) => {
                  this.checkout(visibility)
                    .then((job) => {
                      resolve(JobQueueHelper.prep(job, this.options.raw))
                    }, reject)
                }, reject)
            }, reject)
        }, reject)
    })
  }

  /**
   * Get the next job from the queue without checking it out of the queue
   *
   * @return {Promise<Job>} Job from the queue, or null if the queue was empty.
   */
  peek () {
    return new Promise((resolve, reject) => {
      const query = {
        deleted: null, // Only fetch jobs that are not finished
        visible: {
          $lte: JobQueueHelper.now() // Only fetch jobs that are visible yet
        }
      }

      const options = {
        sort: {
          _id: 1 // Sort by _id
        },
        lean: true
      }

      this.queue.findOne(query, null, options)
        .then((job) => {
          resolve(JobQueueHelper.prep(job, this.options.raw))
        }, reject)
    })
  }

  /**
   * Extend the visibility window for a checked out job.
   * Optionally specifiy the job completion in percent.
   *
   * @param {string} ack Acknowledge key.
   * @param {number} [visibility=JobQueue.visibility] Visibility window for the
   * checked out job. Overrides the global setting if set. [seconds]
   * @param {number} [percent=0] Value between 0 and 100, indicating the
   * progress in percent.
   * @param {object} [payload=undefined] Optional updated payload
   * @return {Promise<Job>} The updated job, or null if non was found.
   */
  ping (ack, visibility, percent, payload) {
    percent = percent || 0
    visibility = visibility || this.options.visibility

    return new Promise((resolve, reject) => {
      const query = {
        ack: ack,
        deleted: null // Only fetch jobs that are not finished
      }

      // Do not allow extension if visibility window is timed out!
      if (this.options.strictAck) {
        query.visible = {
          $gt: JobQueueHelper.now()
        }
      }

      const update = {
        $set: {
          visible: JobQueueHelper.nowPlusSecs(visibility)
        }
      }

      if (percent) {
        update.$set.progress = percent
      }

      if (payload) {
        update.$set.payload = payload
      }

      const options = {
        sort: {
          _id: 1
        },
        new: true,
        lean: true
      }

      // CosmosDB does not support sorting
      // if (this.options.cosmosDb) {
      //   options.sort = undefined
      // }

      this.queue.findOneAndUpdate(query, update, options)
        .then((result) => {
          resolve(JobQueueHelper.prep(result, this.options.raw))
        }, reject)
    })
  }

  /**
   * Mark a job as finished
   *
   * @param {string} ack Acknowledge key.
   * @return {Promise<Job>} acknowledged job, or null if none found.
   */
  acknowledge (ack) {
    return new Promise((resolve, reject) => {
      const query = {
        ack: ack,
        deleted: null // Only fetch jobs that are not finished
      }

      // Do not allow acknowledge if visibility window is timed out!
      if (this.options.strictAck) {
        query.visible = {
          $gt: JobQueueHelper.now()
        }
      }

      const update = {
        $set: {
          deleted: JobQueueHelper.now()
        }
      }

      const options = {
        sort: {
          _id: 1
        },
        new: true,
        lean: true
      }

      // CosmosDB does not support sorting
      // if (this.options.cosmosDb) {
      //   options.sort = undefined
      // }

      this.queue.findOneAndUpdate(query, update, options)
        .then((result) => {
          if (result === null) {
            reject(new Error('Job not found, or visibility window timed out.'))
            return
          }

          resolve(JobQueueHelper.prep(result, this.options.raw))
        }, reject)
    })
  }

  /**
   * Mark a job as finished, short version of acknowledge()
   *
   * @param {string} ack Acknowledge key.
   * @return {Promise<Job>} acknowledged job, or null if none found.
   */
  ack (ack) {
    return this.acknowledge(ack)
  }

  /**
   * Remove finished jobs from the queue, specifiy the age parameter to only
   * remove jobs that are older than that age.
   *
   * @param {number} [age] Minimum age of jobs that will be removed. [seconds]
   * @return {Promise<number>} number of deleted jobs
   */
  cleanup (age) {
    return new Promise((resolve, reject) => {
      let query = {
        deleted: {
          $ne: null
        }
      }

      if (typeof (age) === 'number') {
        // Use age parameter
        query = {
          deleted: {
            $lt: JobQueueHelper.nowPlusSecs(age * -1)
          }
        }
      }

      this.queue.deleteMany(query)
        .then((result) => {
          if (!result) {
            reject(new Error('MongoDB result was empty.'))
            return
          }

          resolve(result.result.n)
        }, (err) => {
          reject(new Error(err))
        })
    })
  }

  /**
   * Remove all jobs from the dead queue. If no dead queue is configured, the
   * returned promise simply resolves with 0 deleted jobs without any action
   * beeing taken.
   *
   * @return {Promise<number>} number of deleted jobs
   */
  cleanupDead () {
    return new Promise((resolve, reject) => {
      // Check if we even have a dead queue, if not just return
      if (!this.deadQueue) {
        resolve(0)
        return
      }

      this.deadQueue.deleteMany()
        .then((result) => {
          if (!result) {
            reject(new Error('MongoDB result was empty.'))
            return
          }

          resolve(result.result.n)
        }, (err) => {
          reject(new Error(err))
        })
    })
  }

  /**
   * Get a list of jobs in the queue.
   *
   * @param {Object} [filter={}] Mongoose query object, to filter the jobs by.
   * @return {Promise<Job[]>} Array of Jobs or null if non were found.
   */
  get (filter) {
    if (typeof (filter) !== 'object') {
      filter = {}
    }

    return new Promise((resolve, reject) => {
      this.queue.find(filter, null, { sort: { _id: 1 }, lean: true })
        .then((foundJobs) => {
          resolve(JobQueueHelper.prep(foundJobs, this.options.raw))
        }, reject)
    })
  }

  /**
   * Deletes all elements in the queue (and the dead queue if configured),
   * regardless of checked out elements.
   *
   * @return {Promise<number>} number of deleted jobs (on both queues)
   */
  reset () {
    return new Promise((resolve, reject) => {
      this.queue.deleteMany()
        .then((queueResult) => {
          if (!queueResult) {
            reject(new Error('MongoDB result was empty.'))
            return
          }

          if (!this.deadQueue) {
            resolve(queueResult.result.n)
            return
          }

          this.deadQueue.deleteMany()
            .then((deadQueueResult) => {
              if (!deadQueueResult) {
                reject(new Error('MongoDB result was empty.'))
                return
              }

              resolve(queueResult.result.n + deadQueueResult.result.n)
            }, reject)
        }, reject)
    })
  }
}

// Export Module
// -----------------------------------------------------------------------------
module.exports = function (mongoose, name, opts) {
  return new JobQueue(mongoose, name, opts)
}
