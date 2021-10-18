#!/bin/bash

mkdir -p ./keys
pushd ./keys

size=1024
# generate RSA 1024 bits private key
openssl genpkey -out private.pkcs1.der -algorithm RSA -outform DER -pkeyopt "rsa_keygen_bits:${size}"
openssl pkcs8 -topk8 -inform DER -outform DER -nocrypt -in private.pkcs1.der -out private.pkcs8.der
# generate associated public key
openssl rsa -inform DER -outform DER -in private.pkcs1.der -pubout > public.der
# build a proactive compatible public key
echo "RSA" > .tmp
echo "${size}" >> .tmp
cat .tmp public.der > public.proactive_der
rm .tmp

popd
