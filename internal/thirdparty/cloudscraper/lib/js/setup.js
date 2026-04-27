var window = this;
var navigator = { userAgent: "" };
var location = {
    href: "https://" + (__cloudscraper_domain__ || "example.com") + "/",
    hostname: __cloudscraper_domain__ || "example.com",
    host: __cloudscraper_domain__ || "example.com",
    protocol: "https:"
};
var __documentElements = {};
var __collectByTagName = function(node, tagName, out) {
    if (!node || !out) return out;
    var wanted = String(tagName || "*").toUpperCase();
    if (node.tagName && (wanted === "*" || node.tagName === wanted)) {
        out.push(node);
    }
    if (node.children && node.children.length) {
        for (var i = 0; i < node.children.length; i++) {
            __collectByTagName(node.children[i], wanted, out);
        }
    }
    return out;
};
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
        },
        getElementsByTagName: function(tagName) {
            var out = [];
            if (this.children && this.children.length) {
                for (var i = 0; i < this.children.length; i++) {
                    __collectByTagName(this.children[i], tagName, out);
                }
            }
            return out;
        }
    };
    if (node.tagName === "A") {
        node.href = location.href;
        node.firstChild = node;
    }
    return node;
};
var __documentElement = __createElementNode("html");
var __headElement = __createElementNode("head");
var __bodyElement = __createElementNode("body");
__documentElement.appendChild(__headElement);
__documentElement.appendChild(__bodyElement);
var document = {
    documentElement: __documentElement,
    head: __headElement,
    body: __bodyElement,
    getElementById: function(id) {
        if (!__documentElements[id]) {
            __documentElements[id] = __createElementNode("input");
            __documentElements[id].id = id;
            __bodyElement.appendChild(__documentElements[id]);
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
    getElementsByTagName: function(tagName) {
        return __collectByTagName(__documentElement, tagName, []);
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
