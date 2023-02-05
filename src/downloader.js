const async = require('async');
const fs = require('fs');
const { join: pathJoin } = require('path');
const request = require('request');

const { fetchAuth } = require('./auth');

const REQ_TIMEOUT = 10 * 1000;
const MIN_RETRY_MS = 1000;
const MAX_RETRY_MS = 30 * 1000;

exports.downloadFile = downloadFile;

function downloadFile(params, done) {
  const {
    os_auth_url,
    os_password,
    os_username,
    os_tenant_name,
    container,
    object_path,
    dest_path,
    resume,
    overwrite,
  } = params;
  const errorLog = params.error_log || function () {};
  const retry_count = params.retry_count || 1;

  let url;
  let keystone_auth;
  let content_length;
  let try_count = 0;
  let byte_count = 0;
  async.series(
    [
      (done) => {
        const opts = {
          os_auth_url,
          os_password,
          os_username,
          os_tenant_name,
        };
        fetchAuth(opts, (err, token, service_map) => {
          if (err) {
            errorLog('fetchAuth: failed err:', err);
          } else {
            keystone_auth = token;
            if (service_map.swift && service_map.swift[0]) {
              const endpoint_url = service_map.swift[0].publicURL;
              if (!endpoint_url) {
                errorLog('no swift url');
                err = 'no_endpoint';
              } else {
                url = endpoint_url + '/' + pathJoin(container, object_path);
              }
            }
          }
          done(err);
        });
      },
      (done) => {
        const opts = {
          keystone_auth,
          url,
        };
        _headObject(opts, (err, len) => {
          if (err) {
            errorLog('failed to head object:', err);
          }
          content_length = len;
          done(err);
        });
      },
      (done) => {
        const opts = {
          times: retry_count,
          interval: _retryInterval,
          errorFilter: (err) => {
            let retry = true;
            if (err === 'file_exists') {
              retry = false;
            } else if (err === 'conflict') {
              retry = false;
            }
            return retry;
          },
        };
        async.retry(
          opts,
          (done) => {
            const opts = {
              try_count: try_count++,
              dest_path,
              content_length,
              resume,
              overwrite,
              keystone_auth,
              url,
              errorLog,
            };
            _downloadTry(opts, (err, count) => {
              byte_count += count;
              done(err);
            });
          },
          done
        );
      },
    ],
    (err) => {
      done(err, byte_count);
    }
  );
}

function _downloadTry(params, done) {
  const {
    try_count,
    dest_path,
    content_length,
    resume,
    overwrite,
    keystone_auth,
    url,
    errorLog,
  } = params;

  let start_pos = 0;
  let byte_count = 0;
  async.series(
    [
      (done) => {
        fs.stat(dest_path, (err, stats) => {
          if (err && err.code === 'ENOENT') {
            err = null;
          } else if (err) {
            errorLog('stat on file failed:', err);
          } else if (resume || try_count > 0) {
            start_pos = stats.size;
          } else if (!resume && !overwrite) {
            errorLog('file exists without resume or overwrite, aborting');
            err = 'file_exists';
          }
          done(err);
        });
      },
      (done) => {
        if (start_pos === content_length) {
          done();
        } else {
          const opts = {
            try_count,
            keystone_auth,
            url,
            dest_path,
            start: start_pos,
            resume,
            overwrite,
          };
          _download(opts, (err, count) => {
            if (err) {
              errorLog('download failed, err:', err);
            } else {
              byte_count += count;
              if (start_pos + count < content_length) {
                err = 'short';
              }
            }
            done(err);
          });
        }
      },
    ],
    (err) => {
      done(err, byte_count);
    }
  );
}

function _download(params, done) {
  const { keystone_auth, url, dest_path, try_count, start, resume, overwrite } =
    params;
  let byte_count = 0;
  let flags;
  if (try_count > 0) {
    flags = 'r+';
  } else if (overwrite) {
    flags = 'w';
  } else if (resume && start) {
    flags = 'r+';
  } else if (resume) {
    flags = 'w';
  } else {
    flags = 'wx';
  }
  const stream = fs.createWriteStream(dest_path, { flags, start });
  const opts = {
    url,
    method: 'GET',
    headers: {
      'X-Auth-Token': keystone_auth,
    },
    timeout: REQ_TIMEOUT,
  };
  if (start) {
    opts.headers.Range = `bytes=${start}-`;
  }
  function _onDone(err) {
    if (done) {
      done(err, byte_count);
      done = null;
    }
  }
  const req = request(opts);
  req.on('response', (response) => {
    const statusCode = response && response.statusCode;
    if (statusCode >= 300) {
      req.abort();
      let err = statusCode;
      if (statusCode === 409) {
        err = 'conflict';
      }
      if (done) {
        done(err);
        done = null;
      }
    }
  });
  req.on('error', _onDone);
  req.on('finish', _onDone);
  //req.on('close', _onDone);
  req.on('end', _onDone);
  stream.on('error', (err) => {
    if (req) {
      req.abort();
    }
    if (done) {
      done(err);
      done = null;
    }
  });
  req.on('data', (data) => {
    byte_count += data.length;
  });
  req.pipe(stream);
}
function _headObject(params, done) {
  const { keystone_auth, url } = params;
  const req = {
    method: 'HEAD',
    url,
  };
  _send({ req, keystone_auth }, (err, body, response) => {
    let content_length;
    if (response && response.headers) {
      content_length = parseInt(response.headers['content-length']);
      if (isNaN(content_length) || !content_length) {
        err = 'bad_content_length';
      }
    }
    done(err, content_length);
  });
}
function _send(opts, done) {
  const { keystone_auth, req } = opts;
  if (!req.headers) {
    req.headers = {};
  }
  req.headers['X-Auth-Token'] = keystone_auth;
  req.timeout = REQ_TIMEOUT;
  request(req, (err, response, body) => {
    const statusCode = response && response.statusCode;
    if (!err && (statusCode < 200 || statusCode > 299)) {
      err = statusCode;
    }
    done(err, body, response);
  });
}
function _retryInterval(retryCount) {
  return Math.min(MIN_RETRY_MS + 1000 * Math.pow(2, retryCount), MAX_RETRY_MS);
}
