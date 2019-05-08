var net = require("net");
var tls = require('tls');
var fs = require('fs');

module.exports.createProxy = function (proxyPort,
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
  var self = this;
  this.proxyPort = proxyPort;
  this.serviceHosts = parse(serviceHost);
  this.servicePorts = parse(servicePort);
  if(this.serviceHosts.length != this.servicePorts.length) {
    throw new Error("serviceHosts and servicePorts length not match");
  }
  this.serviceHostIndex = -1;
  // 用户Hash信息
  this.hashedServer = {};
  // 服务负载情况
  this.serviceAccepts = [];
  this.servicePorts.forEach(function(){self.serviceAccepts.push(0)});
  if (options === undefined) {
    this.options = { quiet: false };
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
TcpProxy.prototype.createListener = function () {
  var self = this;
  if (self.options.tls !== false) {
    self.server = tls.createServer(self.proxyTlsOptions, function (socket) {
      // socket.setEncoding("hex");
      self.handleClient(socket);
    });
  } else {
    self.server = net.createServer(function (socket) {
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
TcpProxy.prototype.handleClient = function (proxySocket) {
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
  proxySocket.on("data", function (data) {
    if (context.connected) {
      self.log("#recive:" + data);
      context.serviceSocket.write(data, "hex");
    } else {
      context.buffers[context.buffers.length] = data;
    }
  });
  proxySocket.on("close", function (hadError) {
    delete self.proxySockets[uniqueKey(proxySocket)];
    context.serviceSocket.destroy();
  });
  proxySocket.on("error", function (e) {
    self.log(e);
    context.serviceSocket.destroy();
  });
};

/**
 * 创建转发服务
 * @param context
 */
TcpProxy.prototype.createServiceSocket = function (context) {
  var self = this;
  var i = self.getServiceHostIndex(context.key);
  if (self.options.tls === "both") {
    context.serviceSocket = tls.connect(self.servicePorts[i],
      self.serviceHosts[i], self.serviceTlsOptions, function () {
        self.writeBuffer(context);
      });

  } else {
    context.serviceSocket = new net.Socket();
    context.serviceSocket.setEncoding("hex");
    context.serviceSocket.connect(self.servicePorts[i],
      self.serviceHosts[i], function () {
        self.writeBuffer(context);
      });
  }
  context.serviceSocket.on("data", function (data) {
    self.log("#Response:" + data);
    context.proxySocket.write(data, "hex");
  });
  context.serviceSocket.on("close", function (hadError) {
    context.proxySocket.destroy();
  });
  context.serviceSocket.on("error", function (e) {
    context.proxySocket.destroy();
  });
};

/**
 * 获取服务器序号
 * @param key
 * @returns {*|number}
 */
TcpProxy.prototype.getServiceHostIndex = function (key) {
  var self = this;
  var idx = self.getHashedHostIndex(key);
  if (idx != -1) {
    self.log(`Proxy: ${key} to ${self.serviceHosts[idx]}:${self.servicePorts[idx]} from Hash`);
    return idx;
  } else {
    // 释放超时Client
    this.releaseTimeoutHash();
    this.loadBalanceWithMaxAcceptCheck(key);
    return this.serviceHostIndex;
  }
};

/**
 * 根据每台主机最大负载进行负载调度
 */
TcpProxy.prototype.loadBalanceWithMaxAcceptCheck = function(key) {
  var self = this;
  var startIdex = this.serviceHostIndex;
  do {
    this.serviceHostIndex++;
    if (this.serviceHostIndex == this.serviceHosts.length) {
      this.serviceHostIndex = 0;
    }
  } while (this.serviceAccepts[this.serviceHostIndex] >= this.options.maxAccepts && startIdex != this.serviceHostIndex)
  if(this.serviceAccepts[this.serviceHostIndex] >= this.options.maxAccepts) {
    // 服务都已满的情况下，进行简单轮询
    console.error('#WARNING: Service full');
    this.serviceHostIndex++;
    if (this.serviceHostIndex == this.serviceHosts.length) {
      this.serviceHostIndex = 0;
    }
  }
  this.setHashedHostIndex(key, this.serviceHostIndex);
  this.log(`Proxy: ${key} to ${self.serviceHosts[this.serviceHostIndex]}:${self.servicePorts[this.serviceHostIndex]}`);
  return this.serviceHostIndex;
}

/**
 * 释放HASH
 */
TcpProxy.prototype.releaseTimeoutHash = function () {
  var time = Date.now();
  for (var key in this.hashedServer) {
    var hash = this.hashedServer[key];
    if (time - hash.t > this.options.maxHashTime) {
      this.log(`#Release ${key}`);
      this.serviceAccepts[hash.i]--;
      delete this.hashedServer[key];

    }
  }
}

/**
 * 创建HASH
 * @param key
 * @param idx
 */
TcpProxy.prototype.setHashedHostIndex = function (key, idx) {
  this.log(`#Hash ${key} on Server ${idx}`);
  this.hashedServer[key] = { i: idx, t: Date.now() };
  this.serviceAccepts[idx]++;
}

/**
 * 尝试获取已HASH服务器Index
 * @param key
 * @returns {int}
 */
TcpProxy.prototype.getHashedHostIndex = function (key) {
  var server = this.hashedServer[key];
  if (server != null) {
    // 刷新Hash时间
    server.t = Date.now();
    // console.log(JSON.stringify(this.hashedServer));
    // console.log(JSON.stringify(this.serviceAccepts));
    return server.i;
  }
  return -1;
}

TcpProxy.prototype.writeBuffer = function (context) {
  context.connected = true;
  if (context.buffers.length > 0) {
    for (var i = 0; i < context.buffers.length; i++) {
      this.log("#write:" + context.buffers[i]);
      context.serviceSocket.write(context.buffers[i], "hex");
    }
  }
};

TcpProxy.prototype.end = function () {
  this.server.close();
  for (var key in this.proxySockets) {
    this.proxySockets[key].destroy();
  }
  this.server.unref();
};

TcpProxy.prototype.log = function (msg) {
  if (!this.options.quiet) {
    console.log(msg);
  }
};
