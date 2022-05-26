module github.com/birdayz/streamz

go 1.18

require (
	github.com/alecthomas/assert/v2 v2.0.3
	github.com/docker/go-connections v0.4.0
	github.com/go-logr/logr v1.2.3
	github.com/testcontainers/testcontainers-go v0.13.1-0.20220519090050-154b938070bd
	github.com/twmb/franz-go v1.4.3-0.20220501054214-8148c55adcd2
	github.com/twmb/franz-go/pkg/kadm v0.0.0-20220215213838-c67ef7e57058
	github.com/twmb/franz-go/pkg/kmsg v1.1.0
	go.uber.org/multierr v1.8.0
	golang.org/x/exp v0.0.0-20220414153411-bcd21879b8fd
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
)

require (
	github.com/Azure/go-ansiterm v0.0.0-20210617225240-d185dfc1b5a1 // indirect
	github.com/Microsoft/go-winio v0.5.1 // indirect
	github.com/Microsoft/hcsshim v0.9.2 // indirect
	github.com/alecthomas/repr v0.1.0 // indirect
	github.com/cenkalti/backoff/v4 v4.1.2 // indirect
	github.com/containerd/cgroups v1.0.3 // indirect
	github.com/containerd/containerd v1.6.1 // indirect
	github.com/docker/distribution v2.7.1+incompatible // indirect
	github.com/docker/docker v20.10.11+incompatible // indirect
	github.com/docker/go-units v0.4.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/hexops/gotextdiff v1.0.3 // indirect
	github.com/klauspost/compress v1.15.1 // indirect
	github.com/magiconair/properties v1.8.5 // indirect
	github.com/moby/sys/mount v0.2.0 // indirect
	github.com/moby/sys/mountinfo v0.5.0 // indirect
	github.com/moby/term v0.0.0-20210619224110-3f7ff695adc6 // indirect
	github.com/morikuni/aec v1.0.0 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.0.3-0.20211202183452-c5a74bcca799 // indirect
	github.com/opencontainers/runc v1.1.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.14 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/sirupsen/logrus v1.8.1 // indirect
	github.com/stretchr/testify v1.7.1 // indirect
	github.com/twmb/go-rbtree v1.0.0 // indirect
	go.opencensus.io v0.23.0 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	golang.org/x/net v0.0.0-20220225172249-27dd8689420f // indirect
	golang.org/x/sys v0.0.0-20211216021012-1d35b9e2eb4e // indirect
	google.golang.org/genproto v0.0.0-20211208223120-3a66f561d7aa // indirect
	google.golang.org/grpc v1.43.0 // indirect
	google.golang.org/protobuf v1.27.1 // indirect
	gopkg.in/yaml.v3 v3.0.0 // indirect
)

replace github.com/containerd/containerd => github.com/containerd/containerd v1.6.4

replace github.com/docker/distribution => github.com/distribution/distribution v2.8.1+incompatible

replace gopkg.in/yaml.v3 => gopkg.in/yaml.v3 v3.0.0
