var window = this;
var navigator = { userAgent: "" };
var location = {
    href: "https://" + (__cloudscraper_domain__ || "example.com") + "/",
    hostname: __cloudscraper_domain__ || "example.com",
    host: __cloudscraper_domain__ || "example.com",
    protocol: "https:"
};
var __documentElements = {};
var __createElementNode = function(tag) {
    var node = {
        tagName: String(tag || "").toUpperCase(),
        style: {},
        children: [],
        value: "",
        innerHTML: "",
        textContent: "",
        firstChild: { href: location.href },
        appendChild: function(child) {
            this.children.push(child);
            if (!this.firstChild) this.firstChild = child;
            return child;
        },
        setAttribute: function(name, value) {
            this[name] = value;
        },
        getAttribute: function(name) {
            return this[name];
        }
    };
    if (node.tagName === "A") {
        node.href = location.href;
        node.firstChild = node;
    }
    return node;
};
var document = {
    getElementById: function(id) {
        if (!__documentElements[id]) {
            __documentElements[id] = __createElementNode("input");
            __documentElements[id].id = id;
        }
        return __documentElements[id];
    },
    querySelector: function(selector) {
        if (typeof selector !== "string") return null;
        if (selector.charAt(0) === "#") {
            return this.getElementById(selector.slice(1));
        }
        return null;
    },
    createElement: function(tag) {
        return __createElementNode(tag);
    },
    cookie: ""
};
window.location = location;
window.document = document;
window.setTimeout = function(callback) {
    if (typeof callback === "function") {
        callback();
    }
    return 1;
};
window.clearTimeout = function() {};
var atob = function(str) {
    var chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=';
    var a, b, c, d, e, f, g, i = 0, result = '';
    str = str.replace(/[^A-Za-z0-9\+\/\=]/g, '');
    do {
        a = chars.indexOf(str.charAt(i++)); b = chars.indexOf(str.charAt(i++)); c = chars.indexOf(str.charAt(i++)); d = chars.indexOf(str.charAt(i++));
        e = a << 18 | b << 12 | c << 6 | d; f = e >> 16 & 255; g = e >> 8 & 255; a = e & 255;
        result += String.fromCharCode(f);
        if (c != 64) result += String.fromCharCode(g);
        if (d != 64) result += String.fromCharCode(a);
    } while (i < str.length);
    return result;
};
