ARG VOLCANO_SEED_REPO_URL=https://github.com/volcano-sh/volcano.git

FROM alpine:3.20 AS volcano-source
ARG VOLCANO_SEED_REPO_URL
RUN apk add --no-cache git ca-certificates
RUN set -eu; \
    git clone --mirror "${VOLCANO_SEED_REPO_URL}" /tmp/volcano.git; \
    mkdir -p /volcano/.git; \
    cp -a /tmp/volcano.git/. /volcano/.git/; \
    git --git-dir=/volcano/.git config core.bare false; \
    git --git-dir=/volcano/.git config --unset-all remote.origin.mirror || true; \
    git --git-dir=/volcano/.git config --replace-all remote.origin.url "${VOLCANO_SEED_REPO_URL}"; \
    git --git-dir=/volcano/.git config --replace-all remote.origin.fetch '+refs/heads/*:refs/remotes/origin/*'; \
    git -C /volcano reset --hard HEAD; \
    git -C /volcano for-each-ref --format='%(refname:strip=2) %(objectname)' refs/heads | \
      while read -r branch object; do \
        git -C /volcano update-ref "refs/remotes/origin/${branch}" "${object}"; \
      done; \
    default_branch="$(git -C /volcano symbolic-ref --short HEAD)"; \
    git -C /volcano symbolic-ref refs/remotes/origin/HEAD "refs/remotes/origin/${default_branch}"; \
    rm -rf /tmp/volcano.git

FROM golang:1.22-alpine AS build
WORKDIR /src
COPY . .
RUN go env -w GOPROXY=https://goproxy.cn,direct
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/volens ./cmd/volens

FROM alpine:3.20
RUN apk add --no-cache ca-certificates git openssh-client
RUN addgroup -S volens && adduser -S -G volens -h /var/lib/volens volens
ENV VOLENS_SOURCE_DIR=/var/lib/volens/volcano
COPY --from=build /out/volens /usr/local/bin/volens
COPY --from=volcano-source --chown=volens:volens /volcano ${VOLENS_SOURCE_DIR}
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
COPY skills /opt/volens/skills
RUN chmod +x /usr/local/bin/entrypoint.sh && \
    mkdir -p /var/lib/volens/worktrees && \
    chown volens:volens /var/lib/volens /var/lib/volens/worktrees
USER volens
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
