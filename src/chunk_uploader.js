const _ = require('lodash');
const async = require('async');
const crypto = require('crypto');
const fs = require('fs');
const { join: pathJoin } = require('path');
const request = require('request');

exports.sendChunks = sendChunks;

const REQ_TIMEOUT = 2 * 60 * 1000;
const BACKOFF_RETRY_MS = 60 * 1000;
const g_ipErrorMap = {};

function sendChunks(params, done) {
  const {
    source_path,
    endpoint_url,
    keystone_auth,
    container,
    chunk_list,
    ip_list,
    send_per_ip,
    delete_at,
    error_log,
  } = params;

  const max_inflight_count = ip_list.length
    ? ip_list.length * send_per_ip
    : send_per_ip;

  let inflight_list = [];
  const unsent_list = chunk_list.slice();

  let is_complete = false;
  let byte_count = 0;

  function _startSends() {
    if (_isSendDone(chunk_list)) {
      if (!is_complete) {
        is_complete = true;
        done(null, byte_count);
      }
    } else if (inflight_list.length < max_inflight_count) {
      const { chunk, ip } = _findNextChunk({
        inflight_list,
        unsent_list,
        ip_list,
      });
      if (chunk) {
        const opts = {
          endpoint_url,
          keystone_auth,
          container,
          source_path,
          chunk,
          ip,
          delete_at,
          error_log,
        };
        const chunk_request_number = _sendChunk(opts, _chunkDone);
        inflight_list.push({ chunk_request_number, ip, chunk });
        setImmediate(_startSends);
      }
    }
  }

  function _chunkDone(err, chunk_request_number, chunk) {
    inflight_list = inflight_list.filter(
      (inflight) => inflight.chunk_request_number !== chunk_request_number
    );
    if (!chunk.is_done) {
      unsent_list.push(chunk);
    } else {
      byte_count += chunk.size;
    }
    _startSends();
  }

  _startSends();
}

function _isSendDone(chunk_list) {
  return chunk_list.every((chunk) => chunk.is_done);
}

function _findNextChunk({ inflight_list, unsent_list, ip_list }) {
  const ret = {};
  if (unsent_list.length > 0) {
    ret.chunk = unsent_list.pop();
    ret.ip = _findLeastUsedIp({ inflight_list, ip_list });
  } else if (inflight_list.length > 0 && ip_list.length > 1) {
    const { chunk, ip } = _findDoubleSend({ inflight_list, ip_list });
    if (chunk) {
      ret.chunk = chunk;
      ret.ip = ip;
    }
  }
  return ret;
}
function _findDoubleSend({ inflight_list, ip_list }) {
  let ret = {};
  const sending_list = [];
  inflight_list.forEach(({ ip, chunk }) => {
    const { index, is_done } = chunk;
    if (!is_done) {
      const found = sending_list.find(
        (send_chunk) => send_chunk.index === index
      );
      if (found) {
        found.ip_list.push(ip);
      } else {
        sending_list.push({ index, chunk, ip_list: [ip] });
      }
    }
  });
  if (sending_list.length > 0) {
    sending_list.sort(_sortInflightCount);
    const resend = sending_list[0];
    if (resend.ip_list.length < ip_list.length) {
      const diff_list = _.difference(ip_list, resend.ip_list);
      if (diff_list.length > 0) {
        ret.chunk = resend.chunk;
        ret.ip = diff_list[0];
      }
    }
  }
  return ret;
}

function _findLeastUsedIp({ inflight_list, ip_list }) {
  let ret;
  if (ip_list.length > 0) {
    const used_list = ip_list.map((ip) => ({ ip, count: 0 }));
    inflight_list.forEach(({ ip }) => {
      const found = used_list.find((used) => used.ip === ip);
      if (found) {
        found.count++;
      }
    });
    used_list.sort(_sortCount);
    ret = used_list[0].ip;
  }
  return ret;
}

let last_request_number = 0;
function _sendChunk(params, done) {
  const {
    endpoint_url,
    keystone_auth,
    container,
    source_path,
    chunk,
    ip,
    delete_at,
  } = params;
  const errorLog = params.error_log;
  const stream_opts = {
    start: chunk.start,
    end: chunk.end,
  };
  const chunk_request_number = last_request_number++;

  let url;
  async.series(
    [
      (done) => {
        if (chunk.etag) {
          done();
        } else {
          const body = fs.createReadStream(source_path, stream_opts);
          _hashFile(body, (err, hash) => {
            chunk.etag = hash;
            done(err);
          });
        }
      },
      (done) => {
        if (!chunk.object_path) {
          chunk.object_path = 'segments/' + chunk.etag;
        }
        url = pathJoin(container, chunk.object_path);

        const opts = {
          url,
          etag: chunk.etag,
          endpoint_url,
          keystone_auth,
          delete_at,
        };
        _checkChunk(opts, (err, is_done) => {
          if (err) {
            errorLog('check chunk err:', err);
          } else if (is_done) {
            chunk.is_done = true;
          }
          done(err);
        });
      },
      (done) => {
        if (chunk.is_done) {
          done();
        } else {
          const body = fs.createReadStream(source_path, stream_opts);
          const req = {
            url,
            method: 'PUT',
            body,
            headers: {
              Etag: chunk.etag,
            },
          };
          if (delete_at) {
            req.headers['X-Delete-At'] = delete_at;
          }
          if (ip) {
            req.localAddress = ip;
          }
          const opts = {
            endpoint_url,
            keystone_auth,
            req,
          };
          _send(opts, (err, body) => {
            if (err) {
              errorLog('send chunk err:', err.code ? err.code : err, body);
            } else {
              chunk.is_done = true;
            }
            done(err);
          });
        }
      },
    ],
    (err) => {
      if (err) {
        // backoff on retry
        setTimeout(() => {
          g_ipErrorMap[ip] = (g_ipErrorMap[ip] || 0) + 1;
          done(err, chunk_request_number, chunk);
        }, BACKOFF_RETRY_MS);
      } else {
        done(err, chunk_request_number, chunk);
      }
    }
  );

  return chunk_request_number;
}
function _checkChunk(params, done) {
  const { url, etag, endpoint_url, keystone_auth, delete_at } = params;
  let is_done = false;
  let existing_delete_at;
  async.series(
    [
      (done) => {
        const opts = {
          req: {
            method: 'HEAD',
            url,
          },
          endpoint_url,
          keystone_auth,
        };
        _send(opts, (err, _, response) => {
          if (err === 404) {
            is_done = false;
            err = null;
          } else if (!err) {
            const existing_etag = response.headers.etag;
            if (existing_etag === etag) {
              is_done = true;
              const delete_s = response.headers['x-delete-at'];
              existing_delete_at = parseInt(delete_s || '0');
            }
          }
          done(err);
        });
      },
      (done) => {
        if (!is_done) {
          done();
        } else if (!delete_at && !existing_delete_at) {
          done();
        } else if (existing_delete_at >= delete_at) {
          done();
        } else {
          const opts = {
            req: {
              method: 'POST',
              url,
              headers: {
                'X-Delete-At': delete_at,
              },
            },
            endpoint_url,
            keystone_auth,
          };
          _send(opts, done);
        }
      },
    ],
    (err) => {
      done(err, is_done);
    }
  );
}

function _sortCount(a, b) {
  let ret = a.count - b.count;
  if (a.count === b.count) {
    const a_error = g_ipErrorMap[a.ip] || 0;
    const b_error = g_ipErrorMap[b.ip] || 0;
    ret = a_error - b_error;
  }
  return ret;
}
function _sortInflightCount(a, b) {
  return a.ip_list.length - b.ip_list.length;
}

function _send(opts, done) {
  const { req, keystone_auth, endpoint_url } = opts;
  if (!req.headers) {
    req.headers = {};
  }
  req.headers['X-Auth-Token'] = keystone_auth;
  req.url = endpoint_url + '/' + req.url;
  req.timeout = REQ_TIMEOUT;

  request(req, (err, response, body) => {
    const statusCode = response && response.statusCode;
    if (!err && (statusCode < 200 || statusCode > 299)) {
      err = statusCode;
    }
    done(err, body, response);
  });
}

function _hashFile(stream, done) {
  let size = 0;
  const hash = crypto.createHash('md5');
  stream.on('error', (err) => {
    if (done) {
      done(err);
      done = null;
    }
  });
  stream.on('data', (chunk) => {
    size += chunk.length;
    hash.update(chunk);
  });
  stream.on('end', () => {
    if (done) {
      done(null, hash.digest('hex'), size);
      done = null;
    }
  });
}
