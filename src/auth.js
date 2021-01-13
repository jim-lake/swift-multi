const request = require('request');

exports.fetchAuth = fetchAuth;

function fetchAuth(params, done) {
  const { os_auth_url, os_password, os_username, os_tenant_name } = params;
  const opts = {
    url: os_auth_url + 'tokens',
    method: 'POST',
    body: {
      auth: {
        passwordCredentials: {
          password: os_password,
          username: os_username,
        },
        tenantName: os_tenant_name,
      },
    },
    json: true,
    timeout: 60 * 1000,
  };
  request(opts, (err, response, body) => {
    const statusCode = response && response.statusCode;
    if (!err && (statusCode < 200 || statusCode > 299)) {
      err = statusCode;
    }
    let token_id;
    const service_map = {};
    if (!err && body) {
      const access = body.access;
      const token = access && access.token;
      token_id = token && token.id;
      const serviceCatalog = access && access.serviceCatalog;
      if (serviceCatalog && serviceCatalog.length > 0) {
        serviceCatalog.forEach((service) => {
          const { name, endpoints } = service;
          service_map[name] = endpoints;
        });
      }
    }
    done(err, token_id, service_map);
  });
}
