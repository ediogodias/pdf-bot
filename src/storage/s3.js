var debug = require('debug')('pdf:s3')
var AWS = require('aws-sdk');
var s3 = require('s3')
var path = require('path')

function createS3Storage(options = {}) {
  if (!options.accessKeyId) {
    throw new Error('S3: No access key given')
  }

  if (!options.secretAccessKey) {
    throw new Error('S3: No secret access key given')
  }

  if (!options.region) {
    throw new Error('S3: No region specified')
  }

  if (!options.bucket) {
    throw new Error('S3: No bucket was specified')
  }

  return function uploadToS3 (localPath, job) {
    return new Promise((resolve, reject) => {
      var awsS3Client = new AWS.S3(Object.assign(options.s3ClientOptions || {},
        {
          s3Options: {
            accessKeyId: options.accessKeyId,
            secretAccessKey: options.secretAccessKey,
            region: options.region,
            signatureVersion: 'v4'
          }
        })
      )
      var client = s3.createClient({
        s3Client: awsS3Client
      });

      var remotePath = (options.path || '')
      if (typeof options.path === 'function') {
        remotePath = options.path(localPath, job)
      }

      var pathSplitted = localPath.split('/')
      var fileName = pathSplitted[pathSplitted.length - 1]
      var fullRemotePath = path.join(remotePath, fileName)

      var uploadOptions = {
        localFile: localPath,

        s3Params: {
          Bucket: options.bucket,
          Key: fullRemotePath,
        },
      }

      debug('Pushing job ID %s to S3 path: %s/%s', job.id, options.bucket, fileName)

      var uploader = client.uploadFile(uploadOptions);
      uploader.on('error', function(err) {
        reject(err)
      });
      uploader.on('end', function(data) {
        resolve({
          path: {
            bucket: uploadOptions.s3Params.Bucket,
            region: options.region,
            key: uploadOptions.s3Params.Key
          }
        })
      });
    })
  }
}

module.exports = createS3Storage
