#!/bin/bash

CODE_DIRECTORY="/root/sd-oss"

if [ ! -d "$CODE_DIRECTORY" ]; then
  mkdir -p "$CODE_DIRECTORY"
else
  echo " $CODE_DIRECTORY already exists"
fi

REQUIRED_VARS=("ACCESS_KEY_ID" "ACCESS_KEY_SECRET" "BUCKET_NAME" "BUCKET_ENDPOINT" "BUCKET_MODEL_DIR")

for VAR in "${REQUIRED_VARS[@]}"; do
  if [ -z "${!VAR}" ]; then
    echo "ERROR: env val $VAR needed。"
    exit 1
  fi
done

AK=${ACCESS_KEY_ID}
SK=${ACCESS_KEY_SECRET}

WEBUI_USER_CONTENT=$(cat <<EOF
export ACCESS_KEY_ID="$AK"
export ACCESS_KEY_SECRET="$SK"
export BUCKET_NAME="${BUCKET_NAME}"
export BUCKET_ENDPOINT="${BUCKET_ENDPOINT}"
export BUCKET_MODEL_DIR="${BUCKET_MODEL_DIR}"
EOF
)

echo "$WEBUI_USER_CONTENT" > /root/sd-oss/webui-user.sh


JSON_CONTENT=$(cat <<EOF
{
  "AccessKeyId": "$AK",
  "AccessKeySecret": "$SK",
}
EOF
)
DIRECTORY="/root/.alibabacloud"

if [ ! -d "$DIRECTORY" ]; then
  mkdir -p "$DIRECTORY"
else
  echo "$DIRECTORY exists"
fi

echo "$JSON_CONTENT" > /root/.alibabacloud/credentials

echo "Credential file:/root/.alibabacloud/credentials"

nohup sudo bash /root/sd-oss/webui.sh -f --listen --port 3389 > output.log 2>&1 &
echo $!
rm -f /root/sd-oss/webui-user.sh
