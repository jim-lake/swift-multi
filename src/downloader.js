const async = require('async');
const fs = require('fs');
const { join: pathJoin } = require('path');
const request = require('request');

const { fetchAuth } = require('./auth');

const REQ_TIMEOUT = 10 * 60 * 1000;

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

  let endpoint_url;
  let keystone_auth;
  let start_pos = 0;
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
              endpoint_url = service_map.swift[0].publicURL;
            }
            if (!endpoint_url) {
              errorLog('no swift url');
              err = 'no_endpoint';
            }
          }
          done(err);
        });
      },
      (done) => {
        fs.stat(dest_path, (err, stats) => {
          if (err && err.code === 'ENOENT') {
            err = null;
          } else if (err) {
            errorLog('stat on file failed:', err);
          } else if (!resume && !overwrite) {
            errorLog('file exists, aborting');
            err = 'file_exists';
          } else if (resume) {
            start_pos = stats.size;
          }
          done(err);
        });
      },
      (done) => {
        const opts = {
          keystone_auth,
          endpoint_url,
          container,
          object_path,
          dest_path,
          start: start_pos,
          resume,
          overwrite,
        };
        _download(opts, (err) => {
          if (err) {
            errorLog('download failed, err:', err);
          }
          done(err);
        });
      },
    ],
    (err) => {
      done(err);
    }
  );
}

function _download(params, done) {
  const {
    keystone_auth,
    endpoint_url,
    container,
    object_path,
    dest_path,
    start,
    resume,
    overwrite,
  } = params;
  let flags;
  if (overwrite) {
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
    url: endpoint_url + '/' + pathJoin(container, object_path),
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
      done(err);
      done = null;
    }
  }

  const req = request(opts);
  req.on('response', (response) => {
    const statusCode = response && response.statusCode;
    if (statusCode >= 300) {
      req.abort();
      if (done) {
        done(statusCode);
        done = null;
      }
    }
  });
  req.on('error', _onDone);
  req.on('finish', _onDone);
  req.on('close', _onDone);
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
  req.pipe(stream);
}
