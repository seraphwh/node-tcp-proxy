var net = require("net");
var tls = require('tls');
var fs = require('fs');

module.exports.createProxy = function(proxyPort,
    serviceHost, servicePort, options) {
    return new TcpProxy(proxyPort, serviceHost, servicePort, options);
};

function uniqueKey(socket) {
    var key = socket.remoteAddress;// + ":" + socket.remotePort;
    return key;
}

function parse(o) {
    if (typeof o === "string") {
        return o.split(",");
    } else if (Array.isArray(o)) {
        return o;
    } else {
        throw new Error("cannot parse object: " + o);
    }
}

function TcpProxy(proxyPort, serviceHost, servicePort, options) {
    this.proxyPort = proxyPort;
    this.serviceHosts = parse(serviceHost);
    this.servicePorts = parse(servicePort);
    this.serviceHostIndex = -1;
    this.hashedServer = {};
    if (options === undefined) {
        this.options = {quiet: false};
    } else {
        this.options = options;
    }
    this.proxyTlsOptions = {
        passphrase: this.options.passphrase,
        secureProtocol: "TLSv1_2_method"
    };
    if (this.options.tls !== false) {
        this.proxyTlsOptions.pfx = fs.readFileSync(this.options.pfx);
    }
    this.serviceTlsOptions = {
        rejectUnauthorized: this.options.rejectUnauthorized,
        secureProtocol: "TLSv1_2_method"
    };
    this.proxySockets = {};

    this.createListener();
}

TcpProxy.prototype.createListener = function() {
    var self = this;
    if (self.options.tls !== false) {
        self.server = tls.createServer(self.proxyTlsOptions, function(socket) {
            self.handleClient(socket);
        });
    } else {
        self.server = net.createServer(function(socket) {
            self.handleClient(socket);
        });
    }
    self.server.listen(self.proxyPort, self.options.hostname);
};

TcpProxy.prototype.handleClient = function(proxySocket) {
    var self = this;
    var key = uniqueKey(proxySocket);
    console.log(`#key:${key}`);
    self.proxySockets[key] = proxySocket;
    var context = {
        key: key,
        buffers: [],
        connected: false,
        proxySocket: proxySocket
    };
    self.createServiceSocket(context);
    proxySocket.on("data", function(data) {
        if (context.connected) {
            context.serviceSocket.write(data);
        } else {
            context.buffers[context.buffers.length] = data;
        }
    });
    proxySocket.on("close", function(hadError) {
        delete self.proxySockets[uniqueKey(proxySocket)];
        context.serviceSocket.destroy();
    });
    proxySocket.on("error", function(e) {
        context.serviceSocket.destroy();
    });
};

TcpProxy.prototype.createServiceSocket = function(context) {
    var self = this;
    var i = self.getServiceHostIndex(context.key);
    console.log(`Idx:${i}`);
    if (self.options.tls === "both") {
        context.serviceSocket = tls.connect(self.servicePorts[i],
            self.serviceHosts[i], self.serviceTlsOptions, function() {
                self.writeBuffer(context);
            });
    } else {
        context.serviceSocket = new net.Socket();
        context.serviceSocket.connect(self.servicePorts[i],
            self.serviceHosts[i], function() {
                self.writeBuffer(context);
            });
    }
    context.serviceSocket.on("data", function(data) {
        context.proxySocket.write(data);
    });
    context.serviceSocket.on("close", function(hadError) {
        context.proxySocket.destroy();
    });
    context.serviceSocket.on("error", function(e) {
        context.proxySocket.destroy();
    });
};

TcpProxy.prototype.getServiceHostIndex = function(key) {
    var self = this;
    var idx = self.getHashedHostIndex(key);
    if(idx != -1) {
        return idx;
    } else {
      this.serviceHostIndex++;
      if (this.serviceHostIndex == this.serviceHosts.length) {
        this.serviceHostIndex = 0;
        this.releaseTimeoutHash();
      }
      this.setHashedHostIndex(key, this.serviceHostIndex);
      return this.serviceHostIndex;
    }
};

TcpProxy.prototype.releaseTimeoutHash = function() {
    var time = Date.now();
    for(var key in this.hashedServer) {
        if(time - this.hashedServer[key].t > this.options.maxHashTime) {
            console.log(`#Release ${key}`);
            delete this.hashedServer[key];
        }
    }
}

TcpProxy.prototype.setHashedHostIndex = function(key, idx) {
  console.log(`#Hash ${key} on Server ${idx}`);
  // TODO for TEST only
  if(key != '::ffff:10.110.40.81') {
    this.hashedServer[key] = {i: idx, t: Date.now()};
  }
}

TcpProxy.prototype.getHashedHostIndex = function(key) {
    var server = this.hashedServer[key];
    if(server != null) {
      server.t = Date.now();
      console.log(`#Found Server:${server.t}`);
      return server.i;
    }
    return -1;
}

TcpProxy.prototype.writeBuffer = function(context) {
    context.connected = true;
    if (context.buffers.length > 0) {
        for (var i = 0; i < context.buffers.length; i++) {
            context.serviceSocket.write(context.buffers[i]);
        }
    }
};

TcpProxy.prototype.end = function() {
    this.server.close();
    for (var key in this.proxySockets) {
        this.proxySockets[key].destroy();
    }
    this.server.unref();
};

TcpProxy.prototype.log = function(msg) {
    if (!this.options.quiet) {
        console.log(msg);
    }
};
