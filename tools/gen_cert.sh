#!/usr/bin/env bash
# tools/gen_cert.sh —— 生成教学用自签证书（TLS 实验）
#
#   bash tools/gen_cert.sh [输出目录=certs]
#   产出：certs/server.crt（证书）+ certs/server.key（私钥）+ certs/ca.crt（自签即自 CA）
#
# 自签 = 证书的签发者是它自己（没有独立 CA 链）：客户端默认跳过校验（教学），
# 或把 server.crt 当 CA 传给 --ca 做「校验开关」演示。私钥权限 600，仅本机实验用。
set -euo pipefail
DIR="${1:-certs}"
mkdir -p "$DIR"

# 10 年有效期、RSA-2048、SAN 绑 localhost/127.0.0.1（现代 TLS 客户端验主机名要看 SAN）
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "$DIR/server.key" -out "$DIR/server.crt" -days 3650 \
  -subj "/CN=localhost/O=online-chat-dev" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

cp "$DIR/server.crt" "$DIR/ca.crt"   # 自签场景：自己的证书就是信任锚
chmod 600 "$DIR/server.key"

echo "已生成："
echo "  $DIR/server.crt  （服务器证书；自签即 CA）"
echo "  $DIR/server.key  （私钥，0600）"
echo "  $DIR/ca.crt      （客户端 --ca 校验用）"
openssl x509 -in "$DIR/server.crt" -noout -subject -dates
