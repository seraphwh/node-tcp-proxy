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

/**
 * 启动服务
 * @param proxyPort
 * @param serviceHost
 * @param servicePort
 * @param options
 * @constructor
 */
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

/**
 * 创建代理监听
 */
TcpProxy.prototype.createListener = function() {
    var self = this;
    if (self.options.tls !== false) {
        console.log("#TLS");
        self.server = tls.createServer(self.proxyTlsOptions, function(socket) {
          // socket.setEncoding("hex");
          self.handleClient(socket);
        });
    } else {
      console.log("#NO TLS");
        self.server = net.createServer(function(socket) {
          socket.setEncoding("hex");
            self.handleClient(socket);
        });
    }
    self.server.listen(self.proxyPort, self.options.hostname);
};

/**
 * 处理客户端请求
 * @param proxySocket
 */
TcpProxy.prototype.handleClient = function(proxySocket) {
    var self = this;
    var key = uniqueKey(proxySocket);
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
            console.log("#recive:" + data);
            context.serviceSocket.write(data, "hex");
        } else {
            context.buffers[context.buffers.length] = data;
        }
    });
    proxySocket.on("close", function(hadError) {
      console.log("#Close:" + hadError);
        delete self.proxySockets[uniqueKey(proxySocket)];
        context.serviceSocket.destroy();
    });
    proxySocket.on("error", function(e) {
      console.error(e);
        context.serviceSocket.destroy();
    });
};

/**
 * 创建转发服务
 * @param context
 */
TcpProxy.prototype.createServiceSocket = function(context) {
    var self = this;
    var i = self.getServiceHostIndex(context.key);
    if (self.options.tls === "both") {
      console.log("#TLS CONN");
        context.serviceSocket = tls.connect(self.servicePorts[i],
            self.serviceHosts[i], self.serviceTlsOptions, function() {
                self.writeBuffer(context);
            });

    } else {
      console.log("#NO TLS CONN");
        context.serviceSocket = new net.Socket();
      context.serviceSocket.setEncoding("hex");
        context.serviceSocket.connect(self.servicePorts[i],
            self.serviceHosts[i], function() {
                self.writeBuffer(context);
            });
    }
    context.serviceSocket.on("data", function(data) {
      console.log("#Response:" + data);
        context.proxySocket.write(data, "hex");
    });
    context.serviceSocket.on("close", function(hadError) {
      console.error("#Targe Server Close:" + hadError);
        context.proxySocket.destroy();
    });
    context.serviceSocket.on("error", function(e) {
      console.error("#Targe Server Error:" + e);
        context.proxySocket.destroy();
    });
};

/**
 * 获取服务器序号
 * @param key
 * @returns {*|number}
 */
TcpProxy.prototype.getServiceHostIndex = function(key) {
    var self = this;
    var idx = self.getHashedHostIndex(key);
    if(idx != -1) {
        console.log(`Proxy: ${key} to ${self.serviceHosts[this.serviceHostIndex]}:${self.servicePorts[this.serviceHostIndex]} from Hash`);
        return idx;
    } else {
      this.serviceHostIndex++;
      if (this.serviceHostIndex == this.serviceHosts.length) {
        this.serviceHostIndex = 0;
      }
      this.releaseTimeoutHash();
      this.setHashedHostIndex(key, this.serviceHostIndex);
      console.log(`Proxy: ${key} to ${self.serviceHosts[this.serviceHostIndex]}:${self.servicePorts[this.serviceHostIndex]}`);
      return this.serviceHostIndex;
    }
};

/**
 * 释放HASH
 */
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
  this.hashedServer[key] = {i: idx, t: Date.now()};
}

TcpProxy.prototype.getHashedHostIndex = function(key) {
    var server = this.hashedServer[key];
    if(server != null) {
      server.t = Date.now();
      return server.i;
    }
    return -1;
}

TcpProxy.prototype.writeBuffer = function(context) {
    context.connected = true;
    if (context.buffers.length > 0) {
        for (var i = 0; i < context.buffers.length; i++) {
          console.log("#write:" + context.buffers[i]);
            context.serviceSocket.write(context.buffers[i], "hex");
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
