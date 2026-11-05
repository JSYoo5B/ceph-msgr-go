#!/bin/bash
# Development-only check of the pinned capture helper image.
# Usage: verify-image.sh REPOSITORY:TAG@sha256:DIGEST OUTPUT_JSON
#
# The pin is the digest, never the tag. The image index is fetched from the
# registry by digest, its raw bytes must hash to that digest, and it must list
# linux/amd64 and linux/arm64 images. What the tag resolves to today is
# recorded for information only. The image is then pulled by digest, and the
# local image must carry that repository digest.
set -euo pipefail
image=$1
output=$2
case "$image" in *@sha256:*) ;; *) echo "the image is not pinned by digest: $image" >&2; exit 1 ;; esac
digest=${image##*@}
named=${image%@*}
repo=${named%:*}
raw=$(mktemp)
trap 'rm -f "$raw" "$raw.inspect"' EXIT
docker buildx imagetools inspect --raw "$repo@$digest" > "$raw"
actual="sha256:$(sha256sum "$raw" | cut -d' ' -f1)"
if test "$actual" != "$digest"; then
    echo "registry content for $repo@$digest hashes to $actual" >&2
    exit 1
fi
docker manifest inspect "$repo@$digest" > "$raw.inspect"
tag_digest=$(docker buildx imagetools inspect "$named" --format '{{json .Manifest}}' | python3 -c 'import json,sys; print(json.load(sys.stdin)["digest"])' || echo unknown)
for attempt in 1 2 3; do
    docker pull -q "$image" >/dev/null && break
    test "$attempt" = 3 && exit 1
    sleep $((attempt * 10))
done
repo_digests=$(docker image inspect --format '{{join .RepoDigests ","}}' "$image")
python3 - "$raw.inspect" "$output" "$image" "$digest" "$tag_digest" "$repo_digests" <<'PY'
import json, sys
inspect, output, image, digest, tag_digest, repo_digests = sys.argv[1:]
index = json.load(open(inspect))
platforms = sorted("%s/%s" % (m["platform"]["os"], m["platform"]["architecture"])
                   for m in index.get("manifests", []) if "platform" in m)
problems = [p for p in ("linux/amd64", "linux/arm64") if p not in platforms]
if not any(d.endswith("@" + digest) for d in repo_digests.split(",")):
    problems.append("pulled image does not carry repository digest " + digest)
json.dump({"image": image, "pinned_index_digest": digest, "index_media_type": index.get("mediaType"),
           "platforms": platforms, "tag_resolves_to_informational": tag_digest,
           "local_repo_digests": repo_digests.split(","), "problems": problems}, open(output, "w"), indent=2)
print(json.dumps(json.load(open(output)), indent=2))
sys.exit(1 if problems else 0)
PY
