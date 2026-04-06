var window = this;
var navigator = { userAgent: "" };
var document = {
    getElementById: function(id) {
        return { value: "" };
    },
    createElement: function(tag) {
        return {
            firstChild: { href: "https://` + domain + `/" }
        };
    },
    cookie: ""
};
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