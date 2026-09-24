// include/chat/minijson.h —— 嵌套 JSON 解析/序列化（自研，无第三方依赖）
//
// 支持：字符串（含 \uXXXX 代理对）、整数、数组、嵌套对象——覆盖本项目全部帧形态
// （HISTORY 条目数组 / ROOMS_LIST 对象数组）。只实现协议所需的 JSON 子集：
// 数字按 long long 解析——取整数部分，【小数与指数部分直接丢弃】（1e5→1；协议字段
// 全为整型，实数输入按截断处理）；不支持 true/false/null 的完整语义（null→空串）。
// 头文件全部 inline，可直接被单测引用。
#ifndef CHAT_MINIJSON_H_
#define CHAT_MINIJSON_H_

#include <cstdio>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace minijson {

struct Object;

struct Value {
    enum Type { T_STR, T_NUM, T_LIST, T_OBJ } type;
    std::string str;
    long long num;
    std::vector<Value> list;
    std::shared_ptr<Object> obj;
    Value() : type(T_STR), num(0) {}
    static Value make_str(const std::string& s) { Value v; v.type = T_STR; v.str = s; return v; }
    static Value make_num(long long n) { Value v; v.type = T_NUM; v.num = n; return v; }
    static Value make_list(const std::vector<std::string>& l) {
        Value v;
        v.type = T_LIST;
        for (size_t i = 0; i < l.size(); ++i) v.list.push_back(make_str(l[i]));
        return v;
    }
    static Value make_obj(const Object& o);
};

struct Object {
public:
    std::vector<std::pair<std::string, Value> > fields;
    void set(const std::string& k, const Value& v) {
        for (size_t i = 0; i < fields.size(); ++i)
            if (fields[i].first == k) { fields[i].second = v; return; }
        fields.push_back(std::make_pair(k, v));
    }
    void set_str(const std::string& k, const std::string& s) { set(k, Value::make_str(s)); }
    void set_num(const std::string& k, long long n) { set(k, Value::make_num(n)); }
    void set_list(const std::string& k, const std::vector<std::string>& l) { set(k, Value::make_list(l)); }
    void set_objs(const std::string& k, const std::vector<Object>& objs) {
        Value v;
        v.type = Value::T_LIST;
        for (size_t i = 0; i < objs.size(); ++i) v.list.push_back(Value::make_obj(objs[i]));
        set(k, v);
    }
    std::string get_str(const std::string& k, const std::string& def = "") const {
        for (size_t i = 0; i < fields.size(); ++i)
            if (fields[i].first == k && fields[i].second.type == Value::T_STR) return fields[i].second.str;
        return def;
    }
    long long get_num(const std::string& k, long long def = 0) const {
        for (size_t i = 0; i < fields.size(); ++i)
            if (fields[i].first == k && fields[i].second.type == Value::T_NUM) return fields[i].second.num;
        return def;
    }
    std::vector<std::string> get_list(const std::string& k) const {
        std::vector<std::string> out;
        for (size_t i = 0; i < fields.size(); ++i) {
            if (fields[i].first != k || fields[i].second.type != Value::T_LIST) continue;
            for (size_t j = 0; j < fields[i].second.list.size(); ++j)
                if (fields[i].second.list[j].type == Value::T_STR)
                    out.push_back(fields[i].second.list[j].str);
        }
        return out;
    }
    std::vector<Object> get_objs(const std::string& k) const {
        std::vector<Object> out;
        for (size_t i = 0; i < fields.size(); ++i) {
            if (fields[i].first != k || fields[i].second.type != Value::T_LIST) continue;
            for (size_t j = 0; j < fields[i].second.list.size(); ++j)
                if (fields[i].second.list[j].type == Value::T_OBJ && fields[i].second.list[j].obj)
                    out.push_back(*fields[i].second.list[j].obj);
        }
        return out;
    }
};

inline Value Value::make_obj(const Object& o) {
    Value v;
    v.type = T_OBJ;
    v.obj.reset(new Object(o));
    return v;
}

// ---------------- 解析 ----------------

inline void skip_ws(const std::string& t, size_t& i) {
    while (i < t.size() && (t[i] == ' ' || t[i] == '\t' || t[i] == '\n' || t[i] == '\r')) ++i;
}

inline bool append_utf8(std::string& out, unsigned long cp) {
    if (cp <= 0x7F) out += (char)cp;
    else if (cp <= 0x7FF) { out += (char)(0xC0 | (cp >> 6)); out += (char)(0x80 | (cp & 0x3F)); }
    else if (cp <= 0xFFFF) {
        out += (char)(0xE0 | (cp >> 12));
        out += (char)(0x80 | ((cp >> 6) & 0x3F));
        out += (char)(0x80 | (cp & 0x3F));
    } else if (cp <= 0x10FFFF) {
        out += (char)(0xF0 | (cp >> 18));
        out += (char)(0x80 | ((cp >> 12) & 0x3F));
        out += (char)(0x80 | ((cp >> 6) & 0x3F));
        out += (char)(0x80 | (cp & 0x3F));
    } else return false;
    return true;
}

inline bool hex4(const std::string& t, size_t pos, unsigned long& out) {
    if (pos + 4 > t.size()) return false;
    out = 0;
    for (size_t k = 0; k < 4; ++k) {
        char h = t[pos + k];
        out <<= 4;
        if (h >= '0' && h <= '9') out |= (unsigned long)(h - '0');
        else if (h >= 'a' && h <= 'f') out |= (unsigned long)(h - 'a' + 10);
        else if (h >= 'A' && h <= 'F') out |= (unsigned long)(h - 'A' + 10);
        else return false;
    }
    return true;
}

inline bool parse_string(const std::string& t, size_t& i, std::string& out) {
    if (i >= t.size() || t[i] != '"') return false;
    ++i;
    out.clear();
    while (i < t.size()) {
        unsigned char c = (unsigned char)t[i];
        if (c == '"') { ++i; return true; }
        if (c == '\\') {
            ++i;
            if (i >= t.size()) return false;
            char e = t[i];
            switch (e) {
                case '"':  out += '"';  ++i; break;
                case '\\': out += '\\'; ++i; break;
                case '/':  out += '/';  ++i; break;
                case 'b':  out += '\b'; ++i; break;
                case 'f':  out += '\f'; ++i; break;
                case 'n':  out += '\n'; ++i; break;
                case 'r':  out += '\r'; ++i; break;
                case 't':  out += '\t'; ++i; break;
                case 'u': {
                    unsigned long cu = 0;
                    if (!hex4(t, i + 1, cu)) return false;
                    i += 5;
                    unsigned long cp = cu;
                    if (cu >= 0xD800 && cu <= 0xDBFF) {
                        if (i + 1 >= t.size() || t[i] != '\\' || t[i + 1] != 'u') return false;
                        unsigned long lo = 0;
                        if (!hex4(t, i + 2, lo)) return false;
                        if (lo < 0xDC00 || lo > 0xDFFF) return false;
                        cp = 0x10000UL + ((cu - 0xD800UL) << 10) + (lo - 0xDC00UL);
                        i += 6;
                    } else if (cu >= 0xDC00 && cu <= 0xDFFF) return false;
                    if (!append_utf8(out, cp)) return false;
                    break;
                }
                default: return false;
            }
        } else if (c < 0x20) return false;
        else { out += t[i]; ++i; }
    }
    return false;
}

inline bool parse_number(const std::string& t, size_t& i, long long& out) {
    bool neg = false;
    if (i < t.size() && t[i] == '-') { neg = true; ++i; }
    if (i >= t.size() || t[i] < '0' || t[i] > '9') return false;
    long long v = 0;
    while (i < t.size() && t[i] >= '0' && t[i] <= '9') { v = v * 10 + (t[i] - '0'); ++i; }
    if (i < t.size() && t[i] == '.') { ++i; while (i < t.size() && t[i] >= '0' && t[i] <= '9') ++i; }
    if (i < t.size() && (t[i] == 'e' || t[i] == 'E')) {
        ++i;
        if (i < t.size() && (t[i] == '+' || t[i] == '-')) ++i;
        while (i < t.size() && t[i] >= '0' && t[i] <= '9') ++i;
    }
    out = neg ? -v : v;
    return true;
}

inline bool parse_value(const std::string& t, size_t& i, Value& out);  // 前置：对象/数组递归

inline bool parse_object_body(const std::string& t, size_t& i, Object& out) {
    ++i;  // 进入时 i 指向 '{'
    out.fields.clear();
    skip_ws(t, i);
    if (i < t.size() && t[i] == '}') { ++i; return true; }
    while (true) {
        skip_ws(t, i);
        std::string key;
        if (!parse_string(t, i, key)) return false;
        skip_ws(t, i);
        if (i >= t.size() || t[i] != ':') return false;
        ++i;
        Value v;
        if (!parse_value(t, i, v)) return false;
        out.set(key, v);
        skip_ws(t, i);
        if (i < t.size() && t[i] == ',') { ++i; continue; }
        if (i < t.size() && t[i] == '}') { ++i; return true; }
        return false;
    }
}

inline bool parse_value(const std::string& t, size_t& i, Value& out) {
    skip_ws(t, i);
    if (i >= t.size()) return false;
    char c = t[i];
    if (c == '"') {
        std::string s;
        if (!parse_string(t, i, s)) return false;
        out = Value::make_str(s);
        return true;
    }
    if (c == '-' || (c >= '0' && c <= '9')) {
        long long n = 0;
        if (!parse_number(t, i, n)) return false;
        out = Value::make_num(n);
        return true;
    }
    if (c == '{') {  // 嵌套对象（HISTORY 条目 / ROOMS_LIST 条目）
        Object o;
        if (!parse_object_body(t, i, o)) return false;
        out = Value::make_obj(o);
        return true;
    }
    if (c == '[') {
        ++i;
        Value arr;
        arr.type = Value::T_LIST;
        skip_ws(t, i);
        if (i < t.size() && t[i] == ']') { ++i; out = arr; return true; }
        while (true) {
            skip_ws(t, i);
            Value item;
            if (!parse_value(t, i, item)) return false;
            arr.list.push_back(item);
            skip_ws(t, i);
            if (i < t.size() && t[i] == ',') { ++i; continue; }
            if (i < t.size() && t[i] == ']') { ++i; out = arr; return true; }
            return false;
        }
    }
    if (t.compare(i, 4, "null") == 0) { i += 4; out = Value::make_str(""); return true; }
    return false;
}

inline bool parse(const std::string& text, Object& out) {
    size_t i = 0;
    skip_ws(text, i);
    if (i >= text.size() || text[i] != '{') return false;
    if (!parse_object_body(text, i, out)) return false;
    skip_ws(text, i);
    return i == text.size();
}

// ---------------- 序列化 ----------------

inline std::string escape_string(const std::string& s) {
    std::string o;
    for (size_t i = 0; i < s.size(); ++i) {
        unsigned char c = (unsigned char)s[i];
        switch (c) {
            case '"':  o += "\\\""; break;
            case '\\': o += "\\\\"; break;
            case '\b': o += "\\b";  break;
            case '\f': o += "\\f";  break;
            case '\n': o += "\\n";  break;
            case '\r': o += "\\r";  break;
            case '\t': o += "\\t";  break;
            default:
                if (c < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    o += buf;
                } else o += (char)c;
        }
    }
    return o;
}

inline void serialize_value(const Value& v, std::string& s) {
    switch (v.type) {
        case Value::T_STR: s += "\"" + escape_string(v.str) + "\""; break;
        case Value::T_NUM: {
            char buf[24];
            std::snprintf(buf, sizeof(buf), "%lld", v.num);
            s += buf;
            break;
        }
        case Value::T_OBJ:
            if (v.obj) {
                s += "{";
                for (size_t i = 0; i < v.obj->fields.size(); ++i) {
                    if (i) s += ",";
                    s += "\"" + escape_string(v.obj->fields[i].first) + "\":";
                    serialize_value(v.obj->fields[i].second, s);
                }
                s += "}";
            } else s += "null";
            break;
        case Value::T_LIST: {
            s += "[";
            for (size_t j = 0; j < v.list.size(); ++j) {
                if (j) s += ",";
                serialize_value(v.list[j], s);
            }
            s += "]";
            break;
        }
    }
}

inline std::string serialize(const Object& obj) {
    std::string s = "{";
    for (size_t i = 0; i < obj.fields.size(); ++i) {
        if (i) s += ",";
        s += "\"" + escape_string(obj.fields[i].first) + "\":";
        serialize_value(obj.fields[i].second, s);
    }
    s += "}";
    return s;
}

}  // namespace minijson

#endif  // CHAT_MINIJSON_H_
